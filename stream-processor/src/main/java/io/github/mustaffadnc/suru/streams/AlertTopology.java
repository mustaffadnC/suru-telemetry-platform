package io.github.mustaffadnc.suru.streams;

import io.github.mustaffadnc.suru.rules.Alert;
import io.github.mustaffadnc.suru.rules.AlertState;
import io.github.mustaffadnc.suru.rules.Observation;
import io.github.mustaffadnc.suru.rules.RuleEngine;
import java.time.Duration;
import java.util.Objects;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Wires the rule processor between the telemetry topic and the alert topic.
 *
 * <p>Both state stores are changelog-backed, which is the point of using stores at all rather than
 * a field. An instance that restarts or takes over a partition after a rebalance restores what the
 * previous one knew: a rule three seconds into a thirty-second debounce stays three seconds in.
 * Starting from scratch would silently re-arm every pending alert and abandon every open one
 * without emitting a resolution, so a rebalance would quietly close incidents nobody had fixed.
 */
public final class AlertTopology {

    /** Source node name. */
    public static final String SOURCE = "telemetry-source";

    /** Processor node name. */
    public static final String PROCESSOR = "rule-processor";

    /** Sink node name. */
    public static final String SINK = "alert-sink";

    private AlertTopology() {}

    /**
     * How the topology is wired.
     *
     * @param telemetryTopic topic the gateway publishes to
     * @param alertTopic topic alerts are published to
     * @param punctuationInterval how often silence is checked for
     * @param catchUpThreshold how far behind records may be before a still-consuming processor is
     *     treated as replaying rather than as watching an outage
     * @param persistentStores RocksDB when true, in-memory when false
     */
    public record Config(
            String telemetryTopic,
            String alertTopic,
            Duration punctuationInterval,
            Duration catchUpThreshold,
            boolean persistentStores) {

        /** Validates the topics and durations. */
        public Config {
            Objects.requireNonNull(telemetryTopic, "telemetryTopic");
            Objects.requireNonNull(alertTopic, "alertTopic");
            Objects.requireNonNull(punctuationInterval, "punctuationInterval");
            Objects.requireNonNull(catchUpThreshold, "catchUpThreshold");
            if (punctuationInterval.isNegative() || punctuationInterval.isZero()) {
                throw new IllegalArgumentException("punctuationInterval must be positive");
            }
        }

        /**
         * Production defaults: RocksDB stores, silence checked every second.
         *
         * <p>The catch-up threshold is a minute, comfortably longer than any staleness rule's
         * limit and far shorter than a backlog worth suppressing for.
         *
         * @param telemetryTopic topic the gateway publishes to
         * @param alertTopic topic alerts are published to
         * @return the configuration
         */
        public static Config production(String telemetryTopic, String alertTopic) {
            return new Config(
                    telemetryTopic,
                    alertTopic,
                    Duration.ofSeconds(1),
                    Duration.ofMinutes(1),
                    true);
        }
    }

    /**
     * Builds the topology.
     *
     * @param engine the rules to run
     * @param config how to wire it
     * @return the topology
     */
    public static Topology build(RuleEngine engine, Config config) {
        Topology topology = new Topology();

        topology.addSource(
                SOURCE,
                new StringDeserializer(),
                new ByteArrayDeserializer(),
                config.telemetryTopic());

        topology.addProcessor(
                PROCESSOR,
                () ->
                        new RuleProcessor(
                                engine, config.punctuationInterval(), config.catchUpThreshold()),
                SOURCE);

        topology.addStateStore(
                store(RuleProcessor.DEVICE_STATE_STORE, Observation.class, config),
                PROCESSOR);
        topology.addStateStore(
                store(RuleProcessor.ALERT_STATE_STORE, AlertState.class, config), PROCESSOR);

        topology.addSink(
                SINK,
                config.alertTopic(),
                new StringSerializer(),
                JsonSerde.of(Alert.class).serializer(),
                PROCESSOR);

        return topology;
    }

    private static <T> StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<String, T>> store(
            String name, Class<T> type, Config config) {
        KeyValueBytesStoreSupplier supplier =
                config.persistentStores()
                        ? Stores.persistentKeyValueStore(name)
                        : Stores.inMemoryKeyValueStore(name);
        return Stores.keyValueStoreBuilder(supplier, Serdes.String(), JsonSerde.of(type));
    }
}
