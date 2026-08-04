package io.github.mustaffadnc.suru.streams;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * JSON serialisation for state stores and the alert topic.
 *
 * <p>JSON rather than a compact binary encoding, and the reason is operational rather than
 * technical: a changelog topic and an alert topic that can be read with {@code
 * kafka-console-consumer} are worth a great deal at three in the morning, when the question is what
 * the processor believed about a device rather than how many bytes it used saying so.
 *
 * <p><b>This is an unmeasured choice.</b> It has not been shown that Jackson is fast enough on this
 * path, only that it is convenient; replacing it with a hand-written serde would be an optimisation
 * with no measurement behind it, which is the mistake this project has already made once and
 * written up. Phase 6's load work is where the comparison belongs.
 *
 * <p>Timestamps are written as ISO-8601 strings, not epoch numbers. Numeric timestamps are the
 * default and they defeat the entire purpose of choosing JSON — a human reading the changelog wants
 * to see when a device was last heard from, not a decimal.
 *
 * @param <T> the type carried
 */
public final class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    // A record written by a newer version carrying a field this one does not know
                    // must not stop the processor. Rolling upgrades add fields; refusing to read
                    // them turns a deployment into an outage.
                    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final Class<T> type;

    private JsonSerde(Class<T> type) {
        this.type = type;
    }

    /**
     * A serde for a type.
     *
     * @param type the class to carry
     * @param <T> the type
     * @return the serde
     */
    public static <T> JsonSerde<T> of(Class<T> type) {
        return new JsonSerde<>(type);
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot serialise " + type.getSimpleName(), e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, type);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot deserialise " + type.getSimpleName(), e);
            }
        };
    }
}
