package io.github.mustaffadnc.suru.ingest;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves an incoming connection to the tenant that owns it.
 *
 * <p>A link carries a whole tenant, not a single vehicle: one ground link routinely relays several
 * aircraft, told apart by MAVLink system id. So the connection establishes <em>who owns this
 * traffic</em> and the frames themselves establish <em>which vehicle</em> — see
 * {@link #deviceIdOf}.
 *
 * <p><b>This is link registration, not authentication.</b> A source address proves nothing: it can
 * be spoofed, and NAT makes it ambiguous. It is enough to attribute traffic and to keep tenants
 * separated in development, and it is deliberately the weakest part of the gateway. Phase 5 adds
 * real device credentials with rotation, at which point this becomes the fallback for local
 * development rather than the mechanism. Anything relying on tenant isolation for security must
 * wait for that.
 *
 * <p>Thread-safe.
 */
public final class DeviceRegistry {

    /** Tenant used when the registry is running in open mode. */
    public static final String DEFAULT_TENANT = "default";

    private final Map<String, String> tenantByHost = new ConcurrentHashMap<>();
    private final boolean openMode;

    private DeviceRegistry(boolean openMode) {
        this.openMode = openMode;
    }

    /**
     * A registry that accepts every connection and attributes it to {@link #DEFAULT_TENANT}.
     *
     * <p>For local development and for pointing SITL at the gateway without ceremony.
     *
     * @return an open registry
     */
    public static DeviceRegistry open() {
        return new DeviceRegistry(true);
    }

    /**
     * A registry that accepts only hosts registered with {@link #register}.
     *
     * @return a closed registry
     */
    public static DeviceRegistry closed() {
        return new DeviceRegistry(false);
    }

    /**
     * Associates a source host with a tenant.
     *
     * @param host source host, e.g. {@code "127.0.0.1"}
     * @param tenantId owning tenant
     * @return this registry, for chaining
     */
    public DeviceRegistry register(String host, String tenantId) {
        tenantByHost.put(host, tenantId);
        return this;
    }

    /**
     * Resolves the tenant owning a connection.
     *
     * @param remote the peer address
     * @return the tenant, or empty if the connection is not accepted
     */
    public Optional<String> resolveTenant(SocketAddress remote) {
        String host = hostOf(remote);
        String registered = host == null ? null : tenantByHost.get(host);
        if (registered != null) {
            return Optional.of(registered);
        }
        return openMode ? Optional.of(DEFAULT_TENANT) : Optional.empty();
    }

    /**
     * Builds the device id for a vehicle seen on a link.
     *
     * <p>MAVLink system id alone is not unique across a fleet — two tenants can both run a vehicle
     * with sysid 1, and so can two links within one tenant. Qualifying it with the link keeps ids
     * distinct without requiring devices to be pre-registered.
     *
     * @param linkId stable identifier for the connection
     * @param systemId MAVLink system id
     * @return the device id
     */
    public static String deviceIdOf(String linkId, int systemId) {
        return linkId + "/sys" + systemId;
    }

    private static String hostOf(SocketAddress address) {
        return address instanceof InetSocketAddress inet ? inet.getAddress().getHostAddress() : null;
    }
}
