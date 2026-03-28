package company.vk.edu.distrib.compute.mandesero.internal;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class HashResolver {

    private static final String DEFAULT_ALGORITHM = "SHA-256";
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private final String algorithm;
    private final Charset charset;

    public HashResolver() {
        this.algorithm = DEFAULT_ALGORITHM;
        this.charset = DEFAULT_CHARSET;
    }

    public HashResolver(String algorithm, Charset charset) {
        validateAlgorithm(algorithm);
        validateCharset(charset);

        this.algorithm = algorithm;
        this.charset = charset;
    }

    public BucketId resolve(String key) {
        validateKey(key);

        try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] hashBytes = digest.digest(key.getBytes(charset));
            return new BucketId(toHex(hashBytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Configured algorithm is no longer available: " + algorithm, e);
        }
    }

    private static void validateAlgorithm(String algorithm) {
        if (algorithm == null || algorithm.isBlank()) {
            throw new IllegalArgumentException("algorithm must not be null or blank");
        }

        try {
            MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Unsupported algorithm: " + algorithm, e);
        }
    }

    private static void validateCharset(Charset charset) {
        if (charset == null) {
            throw new IllegalArgumentException("charset must not be null");
        }
    }

    private static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }
    }

    private static String toHex(byte[] bytes) {
        return HEX_FORMAT.formatHex(bytes);
    }
}
