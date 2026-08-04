package io.github.mustaffadnc.suru.streams.notify;

import io.github.mustaffadnc.suru.rules.Alert;
import io.github.mustaffadnc.suru.streams.JsonSerde;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.random.RandomGenerator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the alert topic and delivers each alert to a sink.
 *
 * <p><b>A separate consumer, not a step in the topology.</b> Delivery talks to something outside
 * the platform's control, over a network, on someone else's availability. Putting that on the
 * stream thread would make an unresponsive webhook stall rule evaluation itself — so a slow
 * incident tool would stop the platform noticing further incidents, which is the failure that
 * matters least being allowed to cause the one that matters most.
 *
 * <p><b>Offsets are committed after delivery.</b> Same discipline as the storage consumer: a crash
 * between the two redelivers an alert, and a duplicate notification is an annoyance where a lost one
 * is a missed incident.
 *
 * <h2>Giving up is a feature</h2>
 *
 * <p>Retries are bounded and then the alert is abandoned with a loud log line. Retrying forever
 * looks safer and is not: because the offset only advances on success, one permanently undeliverable
 * alert would block every alert behind it indefinitely. Losing one alert is bad; losing all
 * subsequent alerts to protect it is worse.
 */
public final class AlertNotifier implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AlertNotifier.class);

    private static final Deserializer<Alert> ALERTS = JsonSerde.of(Alert.class).deserializer();

    private final Consumer<String, byte[]> consumer;
    private final List<AlertSink> sinks;
    private final RetryPolicy retry;
    private final Duration pollTimeout;
    private final RandomGenerator random = RandomGenerator.getDefault();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean loopActive = new AtomicBoolean();

    private final LongAdder delivered = new LongAdder();
    private final LongAdder abandoned = new LongAdder();
    private final LongAdder retries = new LongAdder();
    private final LongAdder undecodable = new LongAdder();

    /**
     * How hard to try.
     *
     * @param attempts total attempts per sink, including the first
     * @param initialBackoff wait before the second attempt
     * @param maxBackoff ceiling on the wait
     */
    public record RetryPolicy(int attempts, Duration initialBackoff, Duration maxBackoff) {

        /** Validates the policy. */
        public RetryPolicy {
            if (attempts < 1) {
                throw new IllegalArgumentException("attempts must be at least 1");
            }
        }

        /** A policy of four attempts, backing off from 200 ms to 5 s. */
        public static RetryPolicy standard() {
            return new RetryPolicy(4, Duration.ofMillis(200), Duration.ofSeconds(5));
        }
    }

    /**
     * Creates a notifier around a supplied consumer, for tests.
     *
     * @param consumer subscribed consumer; this notifier takes ownership and closes it
     * @param sinks where alerts go
     * @param retry how hard to try
     * @param pollTimeout how long each poll waits
     */
    public AlertNotifier(
            Consumer<String, byte[]> consumer,
            List<AlertSink> sinks,
            RetryPolicy retry,
            Duration pollTimeout) {
        this.consumer = consumer;
        this.sinks = List.copyOf(sinks);
        this.retry = retry;
        this.pollTimeout = pollTimeout;
    }

    /**
     * Creates a notifier with a new consumer subscribed to the alert topic.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId consumer group
     * @param alertTopic topic to consume
     * @param sinks where alerts go
     * @return the notifier
     */
    public static AlertNotifier create(
            String bootstrapServers, String groupId, String alertTopic, List<AlertSink> sinks) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Small batches: this consumer is latency-sensitive and its work is one HTTP call per
        // record, so there is nothing for a large fetch to amortise.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(alertTopic));
        return new AlertNotifier(
                consumer, sinks, RetryPolicy.standard(), Duration.ofMillis(500));
    }

    /**
     * Polls once and delivers whatever came back.
     *
     * @return how many alerts were delivered to at least one sink
     */
    public int pollOnce() {
        ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
        if (records.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (ConsumerRecord<String, byte[]> record : records) {
            Alert alert;
            try {
                alert = ALERTS.deserialize(record.topic(), record.value());
            } catch (RuntimeException e) {
                // A record this notifier cannot read will never become readable. Committing past
                // it is the only way the rest of the topic gets delivered.
                undecodable.increment();
                log.error(
                        "undecodable alert at {}-{} offset {}, skipping",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        e);
                continue;
            }
            if (deliverToAll(alert)) {
                count++;
            }
        }

        // Only now. A crash above redelivers the batch, and a duplicate notification is cheaper
        // than a missed one.
        consumer.commitSync();
        return count;
    }

    /** Delivers to every sink, returning whether at least one succeeded. */
    private boolean deliverToAll(Alert alert) {
        boolean any = false;
        for (AlertSink sink : sinks) {
            if (deliverWithRetries(sink, alert)) {
                any = true;
            }
        }
        return any;
    }

    private boolean deliverWithRetries(AlertSink sink, Alert alert) {
        AlertSink.DeliveryException last = null;
        for (int attempt = 1; attempt <= retry.attempts(); attempt++) {
            try {
                sink.deliver(alert);
                delivered.increment();
                return true;
            } catch (AlertSink.DeliveryException e) {
                last = e;
                if (!e.retryable()) {
                    log.error(
                            "permanent failure delivering {} to {}: {}",
                            alert.key(),
                            sink.name(),
                            e.getMessage());
                    break;
                }
                if (attempt < retry.attempts()) {
                    retries.increment();
                    sleep(backoffFor(attempt));
                }
            }
        }
        abandoned.increment();
        log.error(
                "giving up on {} to {} after {} attempt(s); the topic must keep moving",
                alert.key(),
                sink.name(),
                retry.attempts(),
                last);
        return false;
    }

    /**
     * Exponential backoff with full jitter.
     *
     * <p>The jitter is not decoration. Alerts arrive in bursts — one cause takes out several
     * vehicles at once — and a fixed backoff would have every retry for that burst land on the
     * struggling endpoint in the same millisecond, repeatedly, which is how a recovering service is
     * kept down.
     */
    private Duration backoffFor(int attempt) {
        long base = retry.initialBackoff().toMillis() * (1L << (attempt - 1));
        long capped = Math.min(base, retry.maxBackoff().toMillis());
        return Duration.ofMillis(capped == 0 ? 0 : random.nextLong(capped + 1));
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Polls until {@link #close()} is called. */
    public void runUntilClosed() {
        loopActive.set(true);
        try {
            while (running.get()) {
                pollOnce();
            }
        } catch (WakeupException expected) {
            log.info("notifier woken for shutdown");
        } finally {
            loopActive.set(false);
            consumer.close();
        }
    }

    /**
     * Delivery counters.
     *
     * @return a snapshot
     */
    public NotifierStats stats() {
        return new NotifierStats(
                delivered.sum(), abandoned.sum(), retries.sum(), undecodable.sum());
    }

    @Override
    public void close() {
        running.set(false);
        if (loopActive.get()) {
            consumer.wakeup();
        } else {
            consumer.close();
        }
    }

    /**
     * What the notifier has done.
     *
     * @param delivered successful deliveries, counted per sink
     * @param abandoned alerts given up on after exhausting retries or hitting a permanent failure
     * @param retries retry attempts made
     * @param undecodable records that could not be read as alerts and were skipped
     */
    public record NotifierStats(
            long delivered, long abandoned, long retries, long undecodable) {

        @Override
        public String toString() {
            return "delivered=%d abandoned=%d retries=%d undecodable=%d"
                    .formatted(delivered, abandoned, retries, undecodable);
        }
    }
}
