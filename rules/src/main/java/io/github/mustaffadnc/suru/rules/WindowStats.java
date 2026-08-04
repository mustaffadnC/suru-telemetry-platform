package io.github.mustaffadnc.suru.rules;

/**
 * Summary of one metric over a rolling window.
 *
 * @param count how many samples contributed
 * @param min smallest value
 * @param max largest value
 * @param mean arithmetic mean
 * @param standardDeviation population standard deviation
 * @param slopePerMinute least-squares rate of change, in metric units per minute
 */
public record WindowStats(
        int count,
        double min,
        double max,
        double mean,
        double standardDeviation,
        double slopePerMinute) {

    /** An empty summary, for a window with nothing in it. */
    public static final WindowStats EMPTY = new WindowStats(0, 0, 0, 0, 0, 0);

    /**
     * Whether this summary rests on enough samples to be worth acting on.
     *
     * <p>A slope from two samples is a straight line through two points and says nothing about a
     * trend; a rule that fires on it fires on noise. Rules declare their own minimum.
     *
     * @param minimum how many samples a rule needs
     * @return {@code true} when the window holds at least that many
     */
    public boolean hasAtLeast(int minimum) {
        return count >= minimum;
    }
}
