package company.vk.edu.distrib.compute.mandesero.internal;

import org.junit.jupiter.api.Test;

import java.util.concurrent.locks.Lock;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"PMD.UnitTestAssertionsShouldIncludeMessage", "PMD.UnitTestContainsTooManyAsserts"})
class BucketLockManagerTest {
    private static final String BUCKET_HEX = "abcdef123456";
    private static final String OTHER_BUCKET_HEX = "fedcba654321";

    @Test
    void readLock() {
        BucketLockManager lockManager = new BucketLockManager();
        BucketId bucketId = new BucketId(BUCKET_HEX);

        Lock lock = lockManager.readLock(bucketId);

        assertNotNull(lock);
    }

    @Test
    void writeLock() {
        BucketLockManager lockManager = new BucketLockManager();
        BucketId bucketId = new BucketId(BUCKET_HEX);

        Lock lock = lockManager.writeLock(bucketId);

        assertNotNull(lock);
    }

    @Test
    void sameReadLockForSameBucketId() {
        BucketLockManager lockManager = new BucketLockManager();
        BucketId bucketId = new BucketId(BUCKET_HEX);

        Lock first = lockManager.readLock(bucketId);
        Lock second = lockManager.readLock(bucketId);

        assertSame(first, second);
    }

    @Test
    void sameWriteLockForSameBucketId() {
        BucketLockManager lockManager = new BucketLockManager();
        BucketId bucketId = new BucketId(BUCKET_HEX);

        Lock first = lockManager.writeLock(bucketId);
        Lock second = lockManager.writeLock(bucketId);

        assertSame(first, second);
    }

    @Test
    void diffReadLockForDiffBucketId() {
        BucketLockManager lockManager = new BucketLockManager();

        Lock first = lockManager.readLock(new BucketId(BUCKET_HEX));
        Lock second = lockManager.readLock(new BucketId(OTHER_BUCKET_HEX));

        assertNotSame(first, second);
    }

    @Test
    void diffReadWriteForDiffBucketId() {
        BucketLockManager lockManager = new BucketLockManager();

        Lock first = lockManager.writeLock(new BucketId(BUCKET_HEX));
        Lock second = lockManager.writeLock(new BucketId(OTHER_BUCKET_HEX));

        assertNotSame(first, second);
    }

    @Test
    void validate() {
        BucketLockManager lockManager = new BucketLockManager();

        assertThrows(IllegalArgumentException.class, () -> lockManager.readLock(null));
        assertThrows(IllegalArgumentException.class, () -> lockManager.writeLock(null));
    }
}
