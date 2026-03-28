package company.vk.edu.distrib.compute.mandesero.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class BucketLockManager {
    private final ConcurrentMap<BucketId, ReentrantReadWriteLock> locks;

    public BucketLockManager() {
        this.locks = new ConcurrentHashMap<>();
    }

    public Lock readLock(BucketId bucketId) {
        validateBucketId(bucketId);
        return resolveLock(bucketId).readLock();
    }

    public Lock writeLock(BucketId bucketId) {
        validateBucketId(bucketId);
        return resolveLock(bucketId).writeLock();
    }

    private ReentrantReadWriteLock resolveLock(BucketId bucketId) {
        return locks.computeIfAbsent(bucketId, ignored -> new ReentrantReadWriteLock());
    }

    private static void validateBucketId(BucketId bucketId) {
        if (bucketId == null) {
            throw new IllegalArgumentException("bucketId must not be null");
        }
    }
}
