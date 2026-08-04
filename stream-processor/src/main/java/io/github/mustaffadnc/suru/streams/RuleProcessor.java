package io.github.mustaffadnc.suru.streams;

import io.github.mustaffadnc.suru.ingest.kafka.KafkaTelemetryPublisher;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkMetrics;
import io.github.mustaffadnc.suru.protocol.mavlink.MavlinkPayload;
import io.github.mustaffadnc.suru.rules.Alert;
import io.github.mustaffadnc.suru.rules.AlertState;
import io.github.mustaffadnc.suru.rules.AlertStateStore;
import io.github.mustaffadnc.suru.rules.DerivedMetrics;
import io.github.mustaffadnc.suru.rules.DeviceWindows;
import io.github.mustaffadnc.suru.rules.Observation;
import io.github.mustaffadnc.suru.rules.RuleEngine;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains per-device state, runs the rule engine over it, and forwards alerts.
 *
 * <p>Written against the Processor API rather than the DSL for two reasons the DSL cannot serve: it
 * needs two state stores that outlive a window, and it needs a timer.
 *
 * <h2>Why the timer has to be wall clock</h2>
 *
 * <p>Telemetry loss is a condition on records that <em>did not</em> arrive, so no arriving record
 * can ever trigger it. Kafka Streams offers two punctuation clocks and the choice between them is
 * the single most consequential decision in this class.
 *
 * <p>{@link PunctuationType#STREAM_TIME} advances with record timestamps. It is the usual
 * recommendation, because it makes processing deterministic and replayable — and here it fails in
 * the worst possible way. If <em>all</em> telemetry stops, because the gateway died or the network
 * partitioned, stream time stops with it. No punctuation fires, and the platform produces zero
 * telemetry-loss alerts at exactly the moment every device has gone silent. The failure mode of the
 * safe-looking option is total silence during a total outage.
 *
 * <p>{@link PunctuationType#WALL_CLOCK_TIME} always ticks, so it detects that case. Its own hazard
 * is replay: after a restart with lag, historical records are processed at wall-clock speed, and
 * every device looks like it has been quiet for however long the backlog spans. Left alone it would
 * fire a telemetry-loss alert for every device in the fleet on every restart.
 *
 * <h2>Telling replay apart from an outage</h2>
 *
 * <p>Both look identical from the obvious signal — record timestamps far behind the wall clock. The
 * signal that separates them is whether records are arriving <em>at all</em>:
 *
 * <ul>
 *   <li>Replay: records arriving quickly, timestamps far behind. Suppress staleness — the devices
 *       are probably fine and their newer data simply has not been read yet.
 *   <li>Outage: no records arriving, timestamps falling further behind by the second. Evaluate —
 *       this is precisely the case the rule exists for.
 *   <li>Normal: records arriving, timestamps current. Evaluate.
 * </ul>
 *
 * <p>So the guard is "lagging <em>and</em> still consuming", not "lagging". Suppressing on lag
 * alone would reintroduce the stream-time bug through the back door.
 */
public final class RuleProcessor implements Processor<String, byte[], String, Alert> {

    private static final Logger log = LoggerFactory.getLogger(RuleProcessor.class);

    /** Name of the store holding each device's latest metrics. */
    public static final String DEVICE_STATE_STORE = "device-state";

    /** Name of the store holding each rule's phase per device. */
    public static final String ALERT_STATE_STORE = "alert-state";

    /** Name of the store holding rolling windows for metrics any rule needs a trend for. */
    public static final String WINDOW_STORE = "metric-windows";

    private final RuleEngine engine;
    private final Duration punctuationInterval;
    private final Duration catchUpThreshold;
    private final Duration windowSpan;
    private final int windowCapacity;
    private final int minimumWindowSamples;

    /**
     * Base metrics needing a window, read out of the rules rather than configured separately —
     * a trend rule therefore cannot be deployed without its window.
     */
    private final Set<String> windowedMetrics;

    private ProcessorContext<String, Alert> context;
    private KeyValueStore<String, Observation> deviceStates;
    private KeyValueStore<String, AlertState> alertStates;
    private KeyValueStore<String, DeviceWindows> metricWindows;
    private AlertStateStore alertStateAdapter;

    private long recordsSincePunctuation;
    private Instant latestRecordTime = Instant.EPOCH;
    private final ProcessorCounters counters = new ProcessorCounters();

    /**
     * Creates a processor.
     *
     * @param engine the rules to run
     * @param punctuationInterval how often silence is checked for
     * @param catchUpThreshold how far behind the wall clock records may be before a still-consuming
     *     processor is considered to be replaying rather than watching an outage
     */
    public RuleProcessor(
            RuleEngine engine,
            Duration punctuationInterval,
            Duration catchUpThreshold,
            Duration windowSpan,
            int windowCapacity,
            int minimumWindowSamples) {
        this.engine = engine;
        this.punctuationInterval = punctuationInterval;
        this.catchUpThreshold = catchUpThreshold;
        this.windowSpan = windowSpan;
        this.windowCapacity = windowCapacity;
        this.minimumWindowSamples = minimumWindowSamples;
        this.windowedMetrics = DerivedMetrics.windowedMetricsOf(engine);
    }

    @Override
    public void init(ProcessorContext<String, Alert> context) {
        this.context = context;
        this.deviceStates = context.getStateStore(DEVICE_STATE_STORE);
        this.alertStates = context.getStateStore(ALERT_STATE_STORE);
        this.metricWindows = context.getStateStore(WINDOW_STORE);
        this.alertStateAdapter =
                new AlertStateStore() {
                    @Override
                    public AlertState get(String key) {
                        return alertStates.get(key);
                    }

                    @Override
                    public void put(String key, AlertState state) {
                        alertStates.put(key, state);
                    }
                };

        context.schedule(punctuationInterval, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
    }

    @Override
    public void process(Record<String, byte[]> record) {
        String tenantId = header(record, KafkaTelemetryPublisher.HEADER_TENANT);
        String deviceId = header(record, KafkaTelemetryPublisher.HEADER_DEVICE);
        String messageId = header(record, KafkaTelemetryPublisher.HEADER_MESSAGE_ID);
        if (tenantId == null || deviceId == null || messageId == null) {
            counters.unattributable++;
            log.warn("record at offset {} lacks routing headers", offsetDescription());
            return;
        }

        Instant sampleTime = receivedAt(record);
        String key = tenantId + '/' + deviceId;

        Map<String, Double> updates = new HashMap<>();
        MavlinkMetrics.extract(
                Integer.parseInt(messageId), MavlinkPayload.of(record.value()), updates::put);
        updateWindows(key, updates, sampleTime);

        Observation previous = deviceStates.get(key);
        if (previous == null) {
            previous = Observation.empty(tenantId, deviceId, sampleTime);
        }
        // Even with no metrics this advances lastSeen: a heartbeat carries nothing worth storing
        // and is the strongest evidence there is that the device is alive.
        Observation current = previous.withAll(updates, sampleTime);
        deviceStates.put(key, current);

        counters.recordsProcessed++;
        recordsSincePunctuation++;
        if (sampleTime.isAfter(latestRecordTime)) {
            latestRecordTime = sampleTime;
        }

        for (Alert alert : engine.observe(current, alertStateAdapter)) {
            counters.alertsFromRecords++;
            context.forward(new Record<>(alert.key(), alert, record.timestamp()));
        }
    }

    /**
     * Folds any windowed metrics in this record into the device's windows, and adds every derived
     * statistic to the update so the rules see them as ordinary metrics.
     *
     * <p>Nothing happens when no rule asks for a trend, which is the common case: the window store
     * stays empty and the record costs one set lookup.
     */
    private void updateWindows(String key, Map<String, Double> updates, Instant sampleTime) {
        if (windowedMetrics.isEmpty()) {
            return;
        }
        DeviceWindows windows = metricWindows.get(key);
        if (windows == null) {
            windows = DeviceWindows.empty();
        }
        boolean changed = false;
        for (String metric : windowedMetrics) {
            Double value = updates.get(metric);
            if (value != null) {
                windows = windows.with(metric, sampleTime, value, windowSpan, windowCapacity);
                changed = true;
            }
        }
        if (changed) {
            metricWindows.put(key, windows);
        }
        // Derived values are added even when this record carried none of the windowed metrics, so
        // a device's trend stays visible to rules across the messages that do not contain it.
        updates.putAll(windows.derivedMetrics(minimumWindowSamples));
    }

    /** Evaluates every known device against the current wall clock. */
    private void punctuate(long wallClockMillis) {
        Instant now = Instant.ofEpochMilli(wallClockMillis);

        long consumed = recordsSincePunctuation;
        recordsSincePunctuation = 0;

        Duration lag = Duration.between(latestRecordTime, now);
        if (consumed > 0 && lag.compareTo(catchUpThreshold) > 0) {
            // Records are flowing but their timestamps are old: this is a backlog being worked
            // through, not a fleet that has gone quiet. Firing here would alert on every device
            // after every restart.
            counters.punctuationsSuppressed++;
            log.debug("suppressing staleness check while catching up, lag {}", lag);
            return;
        }

        counters.punctuations++;
        try (KeyValueIterator<String, Observation> devices = deviceStates.all()) {
            while (devices.hasNext()) {
                Observation asOf = devices.next().value.asOf(now);
                for (Alert alert : engine.observe(asOf, alertStateAdapter)) {
                    counters.alertsFromPunctuation++;
                    context.forward(new Record<>(alert.key(), alert, wallClockMillis));
                }
            }
        }
    }

    /**
     * Counters for what this processor has done.
     *
     * @return a snapshot
     */
    public ProcessorCounters counters() {
        return counters.copy();
    }

    private String offsetDescription() {
        return context.recordMetadata().map(Object::toString).orElse("unknown");
    }

    private static Instant receivedAt(Record<String, byte[]> record) {
        String nanos = header(record, KafkaTelemetryPublisher.HEADER_RECEIVED_AT);
        if (nanos != null) {
            long value = Long.parseLong(nanos);
            return Instant.ofEpochSecond(
                    Math.floorDiv(value, 1_000_000_000L), Math.floorMod(value, 1_000_000_000L));
        }
        return Instant.ofEpochMilli(record.timestamp());
    }

    private static String header(Record<String, byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Mutable counter block, copied out for reporting. */
    public static final class ProcessorCounters {
        private long recordsProcessed;
        private long unattributable;
        private long alertsFromRecords;
        private long alertsFromPunctuation;
        private long punctuations;
        private long punctuationsSuppressed;

        private ProcessorCounters copy() {
            ProcessorCounters c = new ProcessorCounters();
            c.recordsProcessed = recordsProcessed;
            c.unattributable = unattributable;
            c.alertsFromRecords = alertsFromRecords;
            c.alertsFromPunctuation = alertsFromPunctuation;
            c.punctuations = punctuations;
            c.punctuationsSuppressed = punctuationsSuppressed;
            return c;
        }

        /** @return records consumed */
        public long recordsProcessed() {
            return recordsProcessed;
        }

        /** @return records dropped for missing routing headers */
        public long unattributable() {
            return unattributable;
        }

        /** @return alerts raised by an arriving record */
        public long alertsFromRecords() {
            return alertsFromRecords;
        }

        /** @return alerts raised by the silence timer */
        public long alertsFromPunctuation() {
            return alertsFromPunctuation;
        }

        /** @return silence checks performed */
        public long punctuations() {
            return punctuations;
        }

        /** @return silence checks skipped because the processor was catching up */
        public long punctuationsSuppressed() {
            return punctuationsSuppressed;
        }

        @Override
        public String toString() {
            return "records=%d alerts=%d(+%d timed) punctuations=%d(%d suppressed) unattributable=%d"
                    .formatted(
                            recordsProcessed,
                            alertsFromRecords,
                            alertsFromPunctuation,
                            punctuations,
                            punctuationsSuppressed,
                            unattributable);
        }
    }
}
