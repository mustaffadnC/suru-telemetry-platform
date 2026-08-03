package io.github.mustaffadnc.suru.controlplane.telemetry;

import java.time.Duration;

/**
 * Which stored resolution a query reads from.
 *
 * <p>A chart is a few hundred pixels wide. Answering "a month of battery voltage" from raw samples
 * means reading millions of rows and discarding almost all of them on the way to the browser — the
 * database does the work, the network carries the result, and the display throws it away. The
 * rollups exist so that work is done once at write time; this enum is where a query is matched to
 * the coarsest one that still has the detail being asked for.
 *
 * <p>The rule is one-directional on purpose: a query may read a source finer than it needs and
 * aggregate down, but never one coarser, because there is no honest way to invent detail that was
 * already averaged away.
 */
public enum Resolution {

    /** Every stored sample. */
    RAW("telemetry", "time", Duration.ZERO),

    /** One-minute rollup. */
    MINUTE("telemetry_1m", "bucket", Duration.ofMinutes(1)),

    /** One-hour rollup. */
    HOUR("telemetry_1h", "bucket", Duration.ofHours(1));

    private final String table;
    private final String timeColumn;
    private final Duration nativeBucket;

    Resolution(String table, String timeColumn, Duration nativeBucket) {
        this.table = table;
        this.timeColumn = timeColumn;
        this.nativeBucket = nativeBucket;
    }

    /**
     * The table or continuous aggregate to read.
     *
     * @return relation name
     */
    public String table() {
        return table;
    }

    /**
     * The time column, which differs between the raw table and the rollups.
     *
     * @return column name
     */
    public String timeColumn() {
        return timeColumn;
    }

    /**
     * How wide this source's own buckets are.
     *
     * @return the bucket width, {@link Duration#ZERO} for raw samples
     */
    public Duration nativeBucket() {
        return nativeBucket;
    }

    /**
     * Whether this source stores pre-aggregated values rather than samples.
     *
     * @return {@code true} for the rollups
     */
    public boolean isRollup() {
        return this != RAW;
    }

    /**
     * Chooses the coarsest source that still resolves the requested bucket.
     *
     * @param targetBucket how wide the caller's output buckets will be
     * @return the source to read
     */
    public static Resolution forTargetBucket(Duration targetBucket) {
        if (targetBucket.compareTo(HOUR.nativeBucket) >= 0) {
            return HOUR;
        }
        if (targetBucket.compareTo(MINUTE.nativeBucket) >= 0) {
            return MINUTE;
        }
        return RAW;
    }

    /**
     * The bucket width that fits a time span into a point budget.
     *
     * <p>Rounded up to at least a second: a span short enough to want sub-second buckets is short
     * enough that raw samples are already few, and asking PostgreSQL for microsecond buckets over
     * a wide range is a way to run out of memory rather than a way to get detail.
     *
     * @param span the query's time range
     * @param maxPoints how many points the caller wants back
     * @return the bucket width to group by
     */
    public static Duration bucketFor(Duration span, int maxPoints) {
        long seconds = Math.max(1, span.toSeconds() / Math.max(1, maxPoints));
        return Duration.ofSeconds(seconds);
    }
}
