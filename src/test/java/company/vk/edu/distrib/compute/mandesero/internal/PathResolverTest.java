package company.vk.edu.distrib.compute.mandesero.internal;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"PMD.UnitTestAssertionsShouldIncludeMessage", "PMD.UnitTestContainsTooManyAsserts"})
class PathResolverTest {
    private static final Path ROOT_DIR = Path.of(".data");
    private static final String BUCKET_HEX = "abcdef123456";

    @Test
    void rootDir() {
        PathResolver resolver = new PathResolver(ROOT_DIR);

        assertEquals(ROOT_DIR, resolver.rootDir());
    }

    @Test
    void bucketDirectory() {
        PathResolver resolver = new PathResolver(ROOT_DIR);
        BucketId bucketId = new BucketId(BUCKET_HEX);

        Path expected = ROOT_DIR.resolve("ab").resolve("cd");

        assertEquals(expected, resolver.bucketDirectory(bucketId));
    }

    @Test
    void bucketPath() {
        PathResolver resolver = new PathResolver(ROOT_DIR);
        BucketId bucketId = new BucketId(BUCKET_HEX);

        Path expected = ROOT_DIR
                .resolve("ab")
                .resolve("cd")
                .resolve(BUCKET_HEX);

        assertEquals(expected, resolver.bucketPath(bucketId));
    }

    @Test
    void tempBucketPath() {
        PathResolver resolver = new PathResolver(ROOT_DIR);
        BucketId bucketId = new BucketId(BUCKET_HEX);

        Path expected = ROOT_DIR
                .resolve("ab")
                .resolve("cd")
                .resolve(BUCKET_HEX + ".tmp");

        assertEquals(expected, resolver.tempBucketPath(bucketId));
    }

    @Test
    void sameDirectoryForBucketFiles() {
        PathResolver resolver = new PathResolver(ROOT_DIR);
        BucketId bucketId = new BucketId(BUCKET_HEX);

        Path bucketPath = resolver.bucketPath(bucketId);
        Path tempPath = resolver.tempBucketPath(bucketId);

        assertEquals(bucketPath.getParent(), tempPath.getParent());
    }

    @Test
    void validate() {
        assertThrows(IllegalArgumentException.class, () -> new PathResolver(null));

        PathResolver resolver = new PathResolver(ROOT_DIR);
        assertThrows(IllegalArgumentException.class, () -> resolver.bucketDirectory(null));
        assertThrows(IllegalArgumentException.class, () -> resolver.bucketPath(null));
        assertThrows(IllegalArgumentException.class, () -> resolver.tempBucketPath(null));
    }
}
