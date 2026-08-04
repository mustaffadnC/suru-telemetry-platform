package io.github.mustaffadnc.suru.controlplane.security;

/**
 * What a caller is allowed to do.
 *
 * <p>Three roles rather than a permission matrix, because the platform has exactly three kinds of
 * user and a matrix nobody can hold in their head is a matrix nobody audits.
 */
public enum Role {
    /** Read telemetry, alerts and command history. Cannot change anything. */
    OBSERVER,
    /** Everything an observer can do, plus issuing commands to vehicles. */
    OPERATOR,
    /** Everything an operator can do, plus tenant and device administration. */
    ADMIN;

    /**
     * Whether this role may issue commands to a vehicle.
     *
     * @return {@code true} for {@link #OPERATOR} and {@link #ADMIN}
     */
    public boolean mayCommand() {
        return this == OPERATOR || this == ADMIN;
    }

    /**
     * Whether this role may read.
     *
     * @return {@code true} for every role
     */
    public boolean mayRead() {
        return true;
    }
}
