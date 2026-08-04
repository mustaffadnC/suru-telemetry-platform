package io.github.mustaffadnc.suru.streams.notify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.mustaffadnc.suru.rules.Alert;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;

/**
 * Posts alerts to an HTTP endpoint.
 *
 * <p>Built on the JDK's {@link HttpClient} rather than a client library: the request is one POST
 * with a JSON body, and this is the shape Slack, Teams, PagerDuty and every internal incident tool
 * already accept.
 *
 * <p><b>Both timeouts are set, and they are different things.</b> The connect timeout bounds
 * reaching the host; the request timeout bounds the whole exchange. Without the second, an endpoint
 * that accepts the connection and then never responds holds the notifier thread indefinitely — and
 * because delivery is what gates the offset commit, one hung endpoint would stop every subsequent
 * alert rather than just its own.
 */
public final class WebhookAlertSink implements AlertSink {

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final URI endpoint;
    private final HttpClient client;
    private final Duration requestTimeout;

    /**
     * Creates a sink posting to an endpoint.
     *
     * @param endpoint where to POST
     * @param connectTimeout how long to wait to reach the host
     * @param requestTimeout how long to wait for the whole exchange
     */
    public WebhookAlertSink(URI endpoint, Duration connectTimeout, Duration requestTimeout) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.client =
                HttpClient.newBuilder()
                        .connectTimeout(connectTimeout)
                        // Redirects are not followed. A webhook that answers 302 is misconfigured,
                        // and following it would post the alert somewhere nobody chose.
                        .followRedirects(HttpClient.Redirect.NEVER)
                        .build();
    }

    /**
     * A sink with three- and ten-second timeouts.
     *
     * @param endpoint where to POST
     * @return the sink
     */
    public static WebhookAlertSink to(URI endpoint) {
        return new WebhookAlertSink(endpoint, Duration.ofSeconds(3), Duration.ofSeconds(10));
    }

    @Override
    public void deliver(Alert alert) throws DeliveryException {
        byte[] body;
        try {
            body = MAPPER.writeValueAsBytes(alert);
        } catch (JsonProcessingException e) {
            // The alert cannot be encoded, so no number of retries will change anything.
            throw new DeliveryException("cannot serialise alert " + alert.key(), false, e);
        }

        HttpRequest request =
                HttpRequest.newBuilder(endpoint)
                        .timeout(requestTimeout)
                        .header("Content-Type", "application/json")
                        .header("X-Suru-Alert-Key", alert.key())
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                        .build();

        HttpResponse<Void> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException e) {
            // Connection refused, reset, DNS failure, timeout: all worth another attempt.
            throw new DeliveryException("cannot reach " + endpoint, true, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeliveryException("interrupted delivering to " + endpoint, true, e);
        }

        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            return;
        }
        throw new DeliveryException(
                "%s answered %d for alert %s".formatted(endpoint, status, alert.key()),
                isRetryable(status),
                null);
    }

    /**
     * Whether an HTTP status is worth retrying.
     *
     * <p>5xx and 429 are; other 4xx are not. Retrying a 400 or a 404 wastes the attempt budget that
     * a transient failure would have needed, and does it on every alert — a misconfigured endpoint
     * would otherwise consume the notifier's whole retry capacity permanently.
     *
     * @param status the HTTP status code
     * @return {@code true} when another attempt could succeed
     */
    static boolean isRetryable(int status) {
        return status >= 500 || status == 429 || status == 408;
    }

    @Override
    public String name() {
        return "webhook(" + endpoint.getHost() + ")";
    }
}
