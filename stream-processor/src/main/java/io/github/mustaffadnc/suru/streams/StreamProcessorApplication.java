package io.github.mustaffadnc.suru.streams;

import io.github.mustaffadnc.suru.rules.RuleEngine;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the alert topology.
 *
 * <p>Rules are supplied programmatically for now. Rule storage and CRUD belong to the control
 * plane, and until that exists a fixed set keeps the deployment honest about what it evaluates
 * rather than pretending to be configurable.
 */
public final class StreamProcessorApplication {

    private static final Logger log = LoggerFactory.getLogger(StreamProcessorApplication.class);

    private StreamProcessorApplication() {}

    /**
     * Streams settings tuned for alert latency.
     *
     * <p><b>{@code linger.ms} is the setting that matters, and Kafka Streams' default is wrong for
     * this workload.</b> The plain producer defaults to 0; Streams overrides it to 100, trading
     * latency for batching. Measured end to end on this pipeline, that override <em>was</em> the
     * latency:
     *
     * <pre>
     *   linger.ms = 100 (Streams default)   p50 104.5 ms
     *   linger.ms =   5                     p50   8.3 ms
     *   linger.ms =   0                     p50   4.6 ms
     * </pre>
     *
     * <p>The p50 tracks the setting with a ~4 ms base, so roughly 96 % of the default was a record
     * waiting in a send buffer for a batch that was never going to fill. Alerts are rare by
     * construction — that is what makes them alerts — so batching the alert topic buys nothing.
     *
     * <p>Five rather than zero, because this producer also writes the state-store changelogs, and
     * those carry a record per device update. {@code linger.ms} only delays a send when the batch
     * has not already filled, so under real telemetry load the changelog still batches on
     * {@code batch.size} and gives up almost nothing; when the system is idle, alerts arrive twelve
     * times sooner. Zero would buy another 3.7 ms and give up batching entirely.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param applicationId consumer group and state directory name
     * @return the properties
     */
    public static Properties defaultProperties(String bootstrapServers, String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 5);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        return props;
    }

    /**
     * Starts the topology and blocks until the JVM is asked to stop.
     *
     * @param engine the rules to run
     * @param properties Streams settings, normally from {@link #defaultProperties}
     * @param config how the topology is wired
     */
    public static void run(
            RuleEngine engine, Properties properties, AlertTopology.Config config) {
        KafkaStreams streams = new KafkaStreams(AlertTopology.build(engine, config), properties);
        CountDownLatch stopped = new CountDownLatch(1);

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    log.info("shutting down");
                                    streams.close(Duration.ofSeconds(30));
                                    stopped.countDown();
                                },
                                "streams-shutdown"));

        // Replace the thread rather than shutting the client down. A poisoned record or a
        // transient store error should cost one thread, not the whole processor: with the client
        // down nothing evaluates rules at all, and the telemetry-loss alert — the one whose job is
        // to report absence — goes silent along with everything else.
        streams.setUncaughtExceptionHandler(
                throwable -> {
                    log.error("stream thread died, replacing it", throwable);
                    return StreamThreadExceptionResponse.REPLACE_THREAD;
                });

        streams.start();
        log.info(
                "rule engine running: {} rules, {} → {}",
                engine.rules().size(),
                config.telemetryTopic(),
                config.alertTopic());

        try {
            stopped.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
