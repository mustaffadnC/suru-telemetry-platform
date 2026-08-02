package io.github.mustaffadnc.suru.ingest;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Decides what the gateway accepts when it cannot keep up.
 *
 * <p>Pressure is measured as in-flight publications against a fixed capacity. A publication is
 * in flight from the moment it is handed to the publisher until the broker acknowledges it, so
 * pressure rises the instant the downstream slows — before any queue has grown large enough to
 * matter, and long before memory becomes a problem.
 *
 * <p><b>Why shedding exists at all.</b> The alternative to shedding is unbounded buffering, which
 * does not avoid loss — it defers it, converts it into latency first and an out-of-memory kill
 * later, and loses everything rather than the least important thing. Given that something must be
 * dropped when input outruns output, the only real question is whether the choice is made
 * deliberately and counted, or accidentally and silently.
 *
 * <p><b>Why the two transports behave differently</b> is spelled out in ADR-0003 and matters to
 * every caller: on TCP, refusing to read propagates back to the sender through TCP's own flow
 * control, so the gateway can exert real backpressure and drop nothing. On UDP there is no back
 * channel at all — declining to read simply lets the kernel discard datagrams, uncounted and
 * invisible. So UDP callers must keep reading and shed explicitly here, trading silent loss for
 * measured loss.
 *
 * <p>Thread-safe: consulted from every event loop thread and released from publisher callbacks.
 */
public final class AdmissionController {

    /** Default in-flight capacity. */
    public static final int DEFAULT_CAPACITY = 8192;

    /**
     * Above this pressure a TCP channel stops reading.
     *
     * <p>Set below every shedding threshold in {@link MessagePriority}: pausing is lossless and is
     * therefore always the first remedy. Shedding only begins in the pressure band above this,
     * i.e. once pausing has been applied and has not been enough.
     */
    public static final double HIGH_WATERMARK = 0.60;

    /** Reading resumes once pressure falls back below this. */
    public static final double LOW_WATERMARK = 0.30;

    private final int capacity;
    private final AtomicLong inFlight = new AtomicLong();

    private final LongAdder accepted = new LongAdder();
    private final LongAdder shedCritical = new LongAdder();
    private final LongAdder shedHigh = new LongAdder();
    private final LongAdder shedNormal = new LongAdder();
    private final LongAdder shedBulk = new LongAdder();
    private final AtomicLong peakInFlight = new AtomicLong();

    /** Creates a controller with {@link #DEFAULT_CAPACITY}. */
    public AdmissionController() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Creates a controller.
     *
     * @param capacity in-flight publications treated as full pressure
     * @throws IllegalArgumentException if {@code capacity} is not positive
     */
    public AdmissionController(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive, was " + capacity);
        }
        this.capacity = capacity;
    }

    /**
     * Current pressure.
     *
     * @return {@code 0.0} idle through {@code 1.0} saturated; can exceed 1.0 briefly if callers
     *     admit CRITICAL traffic past capacity, which is intentional
     */
    public double pressure() {
        return (double) inFlight.get() / capacity;
    }

    /**
     * Asks whether a message may be admitted, and reserves capacity for it if so.
     *
     * <p>On {@code true} the caller <b>must</b> eventually call {@link #release()} exactly once.
     *
     * @param priority the message's shedding band
     * @return {@code true} if admitted, {@code false} if shed
     */
    public boolean tryAdmit(MessagePriority priority) {
        if (priority.shouldShedAt(pressure())) {
            countShed(priority);
            return false;
        }
        long current = inFlight.incrementAndGet();
        peakInFlight.accumulateAndGet(current, Math::max);
        accepted.increment();
        return true;
    }

    /** Releases capacity reserved by a successful {@link #tryAdmit}. */
    public void release() {
        inFlight.decrementAndGet();
    }

    /**
     * Whether a transport that can exert real backpressure should stop reading.
     *
     * @return {@code true} above the high watermark
     */
    public boolean shouldPauseReading() {
        return pressure() >= HIGH_WATERMARK;
    }

    /**
     * Whether a paused transport may resume.
     *
     * <p>Deliberately not the inverse of {@link #shouldPauseReading()}: separate watermarks stop
     * the gateway oscillating between reading and not reading on every single message once it
     * settles near the threshold.
     *
     * @return {@code true} below the low watermark
     */
    public boolean mayResumeReading() {
        return pressure() < LOW_WATERMARK;
    }

    private void countShed(MessagePriority priority) {
        switch (priority) {
            case CRITICAL -> shedCritical.increment();
            case HIGH -> shedHigh.increment();
            case NORMAL -> shedNormal.increment();
            case BULK -> shedBulk.increment();
        }
    }

    /**
     * A snapshot of admission statistics.
     *
     * @return current counters
     */
    public AdmissionStats stats() {
        return new AdmissionStats(
                accepted.sum(),
                shedCritical.sum(),
                shedHigh.sum(),
                shedNormal.sum(),
                shedBulk.sum(),
                inFlight.get(),
                peakInFlight.get(),
                capacity);
    }

    /**
     * Configured capacity.
     *
     * @return in-flight publications treated as full pressure
     */
    public int capacity() {
        return capacity;
    }
}
