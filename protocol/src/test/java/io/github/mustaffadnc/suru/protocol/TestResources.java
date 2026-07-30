package io.github.mustaffadnc.suru.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/** Loads files from {@code src/test/resources}. */
public final class TestResources {

    private TestResources() {
        throw new AssertionError("utility class");
    }

    /**
     * Reads a classpath resource as bytes.
     *
     * @param path absolute resource path, e.g. {@code /hk/sample.bin}
     * @return the file contents
     */
    public static byte[] bytes(String path) {
        try (InputStream in = TestResources.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("test resource not found: " + path);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException("failed reading " + path, e);
        }
    }

    /**
     * Reads a classpath resource as UTF-8 text.
     *
     * @param path absolute resource path
     * @return the file contents
     */
    public static String text(String path) {
        return new String(bytes(path), StandardCharsets.UTF_8);
    }
}
