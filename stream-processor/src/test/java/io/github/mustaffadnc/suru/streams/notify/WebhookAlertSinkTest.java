package io.github.mustaffadnc.suru.streams.notify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.mustaffadnc.suru.rules.Alert;
import io.github.mustaffadnc.suru.rules.Severity;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The webhook sink against a real HTTP server.
 *
 * <p>{@link HttpServer} ships with the JDK, so this needs no dependency and no container — and it
 * exercises the actual socket, the actual status handling and the actual timeouts, which a mocked
 * client would not.
 */
class WebhookAlertSinkTest {

    private HttpServer server;
    private final List<String> bodies = new CopyOnWriteArrayList<>();
    private final AtomicInteger status = new AtomicInteger(200);
    private final AtomicInteger delayMillis = new AtomicInteger(0);

    private static Alert alert() {
        return new Alert(
                "battery",
                "Battery low",
                "acme",
                "link/sys1",
                Severity.CRITICAL,
                Alert.Kind.FIRED,
                Instant.parse("2026-08-04T12:00:00Z"),
                Instant.parse("2026-08-04T12:00:04Z"),
                Duration.ofSeconds(30),
                "power.battery_remaining_pct 12.000 (threshold < 20.000)");
    }

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/hook", this::handle);
        server.start();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            bodies.add(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        int wait = delayMillis.get();
        if (wait > 0) {
            try {
                Thread.sleep(wait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        exchange.sendResponseHeaders(status.get(), -1);
        exchange.close();
    }

    private URI endpoint() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/hook");
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("a 200 delivers the alert as JSON")
    void delivers() throws Exception {
        WebhookAlertSink sink = WebhookAlertSink.to(endpoint());

        sink.deliver(alert());

        assertThat(bodies).hasSize(1);
        assertThat(bodies.getFirst())
                .contains("\"ruleId\":\"battery\"")
                .contains("\"deviceId\":\"link/sys1\"")
                .contains("\"severity\":\"CRITICAL\"")
                .contains("\"kind\":\"FIRED\"")
                .as("instants readable rather than epoch numbers, which is why JSON was chosen")
                .contains("2026-08-04T12:00:00Z");
    }

    @Test
    @DisplayName("a 204 counts as delivered")
    void acceptsAnySuccessStatus() throws Exception {
        status.set(204);

        WebhookAlertSink.to(endpoint()).deliver(alert());

        assertThat(bodies).hasSize(1);
    }

    @Test
    @DisplayName("a 500 is reported as retryable")
    void serverErrorIsRetryable() {
        status.set(503);

        assertThatThrownBy(() -> WebhookAlertSink.to(endpoint()).deliver(alert()))
                .isInstanceOf(AlertSink.DeliveryException.class)
                .satisfies(e -> assertThat(((AlertSink.DeliveryException) e).retryable()).isTrue());
    }

    @Test
    @DisplayName("a 400 is reported as permanent")
    void badRequestIsNotRetryable() {
        status.set(400);

        assertThatThrownBy(() -> WebhookAlertSink.to(endpoint()).deliver(alert()))
                .isInstanceOf(AlertSink.DeliveryException.class)
                .satisfies(
                        e -> assertThat(((AlertSink.DeliveryException) e).retryable()).isFalse());
    }

    @Test
    @DisplayName("429 and 408 are retryable even though they are 4xx")
    void rateLimitAndTimeoutAreRetryable() {
        assertThat(WebhookAlertSink.isRetryable(429)).isTrue();
        assertThat(WebhookAlertSink.isRetryable(408)).isTrue();
        assertThat(WebhookAlertSink.isRetryable(404)).isFalse();
        assertThat(WebhookAlertSink.isRetryable(401)).isFalse();
        assertThat(WebhookAlertSink.isRetryable(500)).isTrue();
    }

    /**
     * The failure mode the request timeout exists for.
     *
     * <p>The server accepts the connection and then does not answer. A connect timeout alone would
     * not fire, because connecting succeeded — without a request timeout this call would hold the
     * notifier thread until the server felt like replying, and because delivery gates the offset
     * commit, every alert behind it would wait too.
     */
    @Test
    @DisplayName("an endpoint that accepts and then stalls fails on the request timeout")
    void requestTimeoutBoundsAStalledEndpoint() {
        delayMillis.set(3_000);
        WebhookAlertSink sink =
                new WebhookAlertSink(
                        endpoint(), Duration.ofSeconds(3), Duration.ofMillis(300));

        long start = System.nanoTime();
        assertThatThrownBy(() -> sink.deliver(alert()))
                .isInstanceOf(AlertSink.DeliveryException.class)
                .satisfies(e -> assertThat(((AlertSink.DeliveryException) e).retryable()).isTrue());

        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
        assertThat(elapsed)
                .as("bounded by the request timeout, not by the server's 3 s stall")
                .isLessThan(Duration.ofSeconds(2));
    }

    @Test
    @DisplayName("an unreachable endpoint is retryable")
    void connectionRefusedIsRetryable() {
        // Port 1 on loopback: nothing listens there.
        WebhookAlertSink sink =
                new WebhookAlertSink(
                        URI.create("http://127.0.0.1:1/hook"),
                        Duration.ofMillis(500),
                        Duration.ofSeconds(1));

        assertThatThrownBy(() -> sink.deliver(alert()))
                .isInstanceOf(AlertSink.DeliveryException.class)
                .satisfies(e -> assertThat(((AlertSink.DeliveryException) e).retryable()).isTrue());
    }
}
