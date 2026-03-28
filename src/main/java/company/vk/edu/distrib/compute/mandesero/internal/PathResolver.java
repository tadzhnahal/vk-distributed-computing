package company.vk.edu.distrib.compute.mandesero.internal;

import java.nio.file.Path;

public final class PathResolver {

    private static final String TEMP_FILE_SUFFIX = ".tmp";

    private final Path rootDirectory;

    public PathResolver(Path rootDir) {
        validateRootDir(rootDir);
        this.rootDirectory = rootDir;
    }

    public Path rootDir() {
        return rootDirectory;
    }

    public Path bucketDirectory(BucketId bucketId) {
        validateBucketId(bucketId);
        return rootDirectory.resolve(bucketId.dir1()).resolve(bucketId.dir2());
    }

    public Path bucketPath(BucketId bucketId) {
        validateBucketId(bucketId);
        return bucketDirectory(bucketId).resolve(bucketId.fileName());
    }

    public Path tempBucketPath(BucketId bucketId) {
        validateBucketId(bucketId);
        return bucketDirectory(bucketId).resolve(bucketId.fileName() + TEMP_FILE_SUFFIX);
    }

    private static void validateRootDir(Path rootDir) {
        if (rootDir == null) {
            throw new IllegalArgumentException("rootDir must not be null");
        }
    }

    private static void validateBucketId(BucketId bucketId) {
        if (bucketId == null) {
            throw new IllegalArgumentException("bucketId must not be null");
        }
    }
}
