package company.vk.edu.distrib.compute.mandesero.internal;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"PMD.UnitTestAssertionsShouldIncludeMessage", "PMD.UnitTestContainsTooManyAsserts"})
class HashResolverTest {
    private static final String KEY = "key";
    private static final String DEFAULT_ALGORITHM = "SHA-256";

    @Test
    void resolve() {
        HashResolver resolver = new HashResolver();

        BucketId bucketId1 = resolver.resolve(KEY);
        BucketId bucketId2 = resolver.resolve(KEY);
        BucketId bucketId3 = resolver.resolve("another_key");

        assertEquals(bucketId1, bucketId2);
        assertNotEquals(bucketId1, bucketId3);
    }

    @Test
    void validate() {
        HashResolver resolver = new HashResolver();

        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(null));
        assertThrows(IllegalArgumentException.class, () -> new HashResolver(null, StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> new HashResolver("   ", StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, ()
                -> new HashResolver("NO_SUCH_ALGORITHM", StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> new HashResolver(DEFAULT_ALGORITHM, null));
    }

    @Test
    void resolveWithCustomCharset() {
        HashResolver utf8Resolver = new HashResolver(DEFAULT_ALGORITHM, StandardCharsets.UTF_8);
        HashResolver utf16Resolver = new HashResolver(DEFAULT_ALGORITHM, StandardCharsets.UTF_16BE);

        BucketId utf8BucketId = utf8Resolver.resolve("ключ");
        BucketId utf16BucketId = utf16Resolver.resolve("ключ");

        assertNotEquals(utf8BucketId, utf16BucketId);
    }

    @Test
    void resolveWithCustomAlgorithm() {
        HashResolver sha256Resolver = new HashResolver(DEFAULT_ALGORITHM, StandardCharsets.UTF_8);
        HashResolver sha1Resolver = new HashResolver("SHA-1", StandardCharsets.UTF_8);

        BucketId sha256BucketId = sha256Resolver.resolve(KEY);
        BucketId sha1BucketId = sha1Resolver.resolve(KEY);

        assertNotEquals(sha256BucketId, sha1BucketId);
    }
}
