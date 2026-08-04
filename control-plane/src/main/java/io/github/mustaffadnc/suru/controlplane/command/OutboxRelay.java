package io.github.mustaffadnc.suru.controlplane.command;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes outbox rows towards the vehicles and marks them done.
 *
 * <h2>Publish first, mark second</h2>
 *
 * <p>The relay faces the same two-systems problem the outbox was built to solve, and this time it
 * cannot be solved — only pointed in the direction where the failure is survivable.
 *
 * <ul>
 *   <li><b>Mark first, then publish.</b> A crash between them loses the command permanently and
 *       silently: the row is marked published, no relay will look at it again, and an operator sees
 *       a command that was accepted and dispatched but never left.
 *   <li><b>Publish first, then mark.</b> A crash between them republishes the row later, so the
 *       vehicle may receive the command twice.
 * </ul>
 *
 * <p>The second is chosen for the same reason the storage consumer commits its Kafka offsets after
 * writing to the database: a duplicate is recoverable and a loss is not. It is also what makes the
 * idempotency key load-bearing rather than decorative — the duplicate is expected, not hypothetical.
 *
 * <h2>Why SKIP LOCKED</h2>
 *
 * <p>{@code FOR UPDATE SKIP LOCKED} lets several relay instances run at once, each taking a
 * disjoint batch. Plain {@code FOR UPDATE} would make the second instance block behind the first
 * and add nothing but latency; no locking at all would have both publish the same rows.
 */
public final class OutboxRelay implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    /** Where an outbox row goes. */
    @FunctionalInterface
    public interface Publisher {
        /**
         * Publishes one payload, returning only once the broker has acknowledged it.
         *
         * @param topic destination topic
         * @param key partition key, which is the device so one vehicle's commands stay ordered
         * @param payload the JSON payload
         * @throws Exception if publication failed
         */
        void publish(String topic, String key, String payload) throws Exception;
    }

    private final DataSource dataSource;
    private final Publisher publisher;
    private final int batchSize;
    private final Duration idleWait;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final LongAdder published = new LongAdder();
    private final LongAdder failed = new LongAdder();

    /**
     * Creates a relay.
     *
     * @param dataSource the database holding the outbox
     * @param publisher where rows go
     * @param batchSize how many rows one pass claims
     * @param idleWait how long to wait when the outbox is empty
     */
    public OutboxRelay(
            DataSource dataSource, Publisher publisher, int batchSize, Duration idleWait) {
        this.dataSource = dataSource;
        this.publisher = publisher;
        this.batchSize = batchSize;
        this.idleWait = idleWait;
    }

    /** One row claimed for publication. */
    private record Pending(long id, UUID commandId, String topic, String deviceKey, String payload) {}

    /**
     * Claims a batch, publishes it, and marks what succeeded.
     *
     * @return how many rows were published
     * @throws SQLException if the database work fails
     */
    public int publishBatch() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                List<Pending> batch = claim(connection);
                if (batch.isEmpty()) {
                    connection.commit();
                    return 0;
                }

                List<Pending> sent = new ArrayList<>(batch.size());
                for (Pending row : batch) {
                    try {
                        publisher.publish(row.topic(), row.deviceKey(), row.payload());
                        sent.add(row);
                    } catch (Exception e) {
                        // Leave it unpublished and stop the batch: rows are ordered, and skipping
                        // ahead would deliver a later command to a vehicle before an earlier one.
                        failed.increment();
                        log.warn("publish failed for outbox row {}, will retry", row.id(), e);
                        break;
                    }
                }

                if (!sent.isEmpty()) {
                    markPublished(connection, sent);
                    markCommandsSent(connection, sent);
                    published.add(sent.size());
                }
                connection.commit();
                return sent.size();
            } catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    /**
     * Claims unpublished rows, oldest first, skipping any another relay already holds.
     *
     * <p>The lock is held until this transaction ends, which is what stops a second relay
     * publishing the same rows in the window between reading them and marking them.
     */
    private List<Pending> claim(Connection connection) throws SQLException {
        String sql =
                """
                SELECT o.id, o.command_id, o.topic, o.payload,
                       c.tenant_id || '/' || c.device_id AS device_key
                  FROM command_outbox o
                  JOIN command c ON c.id = o.command_id
                 WHERE o.published_at IS NULL
                 ORDER BY o.id
                 LIMIT ?
                   FOR UPDATE OF o SKIP LOCKED
                """;
        List<Pending> batch = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, batchSize);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    batch.add(
                            new Pending(
                                    rows.getLong("id"),
                                    rows.getObject("command_id", UUID.class),
                                    rows.getString("topic"),
                                    rows.getString("device_key"),
                                    rows.getString("payload")));
                }
            }
        }
        return batch;
    }

    private static void markPublished(Connection connection, List<Pending> sent)
            throws SQLException {
        String sql =
                "UPDATE command_outbox SET published_at = ?, attempts = attempts + 1 WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            Timestamp now = Timestamp.from(Instant.now());
            for (Pending row : sent) {
                statement.setTimestamp(1, now);
                statement.setLong(2, row.id());
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    /**
     * Advances the command to SENT.
     *
     * <p>Guarded on {@code PENDING} so a republished row — which at-least-once delivery makes
     * ordinary — cannot drag a command that has already been answered back out of its settled
     * state.
     */
    private static void markCommandsSent(Connection connection, List<Pending> sent)
            throws SQLException {
        String sql =
                "UPDATE command SET state = 'SENT', updated_at = ? WHERE id = ? AND state = 'PENDING'";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            Timestamp now = Timestamp.from(Instant.now());
            for (Pending row : sent) {
                statement.setTimestamp(1, now);
                statement.setObject(2, row.commandId());
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    /** Publishes until {@link #close()} is called. */
    public void runUntilClosed() {
        while (running.get()) {
            try {
                if (publishBatch() == 0) {
                    sleep(idleWait);
                }
            } catch (SQLException e) {
                log.error("outbox relay pass failed, backing off", e);
                sleep(idleWait);
            }
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Relay counters.
     *
     * @return a snapshot
     */
    public RelayStats stats() {
        return new RelayStats(published.sum(), failed.sum());
    }

    @Override
    public void close() {
        running.set(false);
    }

    /**
     * What the relay has done.
     *
     * @param published rows successfully published
     * @param failed publication attempts that failed and will be retried
     */
    public record RelayStats(long published, long failed) {}
}
