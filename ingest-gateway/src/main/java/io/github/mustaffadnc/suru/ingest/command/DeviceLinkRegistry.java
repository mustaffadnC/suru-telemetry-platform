package io.github.mustaffadnc.suru.ingest.command;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Which link a device is currently reachable on.
 *
 * <p><b>Populated by traffic, not by configuration.</b> A vehicle announces itself by transmitting;
 * until it does, the platform does not know which of its open connections belongs to it, and cannot
 * send it anything. That is a property of the deployment rather than a limitation of this class —
 * MAVLink over a shared link identifies the sender in every frame and nowhere else.
 *
 * <p>Entries are removed when the link closes. A stale entry would be worse than a missing one: a
 * command written to a dead channel fails silently at the socket, where the failure is a log line
 * nobody reads, while a missing entry is an answer the dispatcher can act on.
 *
 * @param <L> the link type, kept generic so this can be tested without Netty
 */
public final class DeviceLinkRegistry<L> {

    /**
     * A device's current link and MAVLink identity.
     *
     * @param link where to write
     * @param systemId the vehicle's MAVLink system id, needed to address a command at it
     * @param componentId the component id
     * @param <L> the link type
     */
    public record Link<L>(L link, int systemId, int componentId) {}

    private final Map<String, Link<L>> links = new ConcurrentHashMap<>();

    private static String key(String tenantId, String deviceId) {
        return tenantId + '/' + deviceId;
    }

    /**
     * Records where a device is reachable, replacing any previous entry.
     *
     * <p>Replacing rather than rejecting: a vehicle that reconnects arrives on a new channel with
     * the same identity, and refusing the new link to protect the old one would leave the device
     * addressable only at a socket that is already closed.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @param systemId the vehicle's MAVLink system id
     * @param componentId the component id
     * @param link where to write
     */
    public void register(String tenantId, String deviceId, int systemId, int componentId, L link) {
        links.put(key(tenantId, deviceId), new Link<>(link, systemId, componentId));
    }

    /**
     * Finds a device's current link.
     *
     * @param tenantId owning tenant
     * @param deviceId the device
     * @return the link, or empty when the device has not been heard from or has disconnected
     */
    public Optional<Link<L>> find(String tenantId, String deviceId) {
        return Optional.ofNullable(links.get(key(tenantId, deviceId)));
    }

    /**
     * Removes every device reachable over a link that has closed.
     *
     * <p>One channel can carry several vehicles — a MAVLink router or a companion computer
     * forwarding a whole airframe — so closing it removes all of them, not one.
     *
     * @param link the closed link
     * @return how many devices became unreachable
     */
    public int unregister(L link) {
        int[] removed = {0};
        links.entrySet()
                .removeIf(
                        entry -> {
                            boolean match = entry.getValue().link().equals(link);
                            if (match) {
                                removed[0]++;
                            }
                            return match;
                        });
        return removed[0];
    }

    /**
     * How many devices are currently reachable.
     *
     * @return the count
     */
    public int size() {
        return links.size();
    }
}
