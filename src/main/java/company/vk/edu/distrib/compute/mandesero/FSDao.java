package company.vk.edu.distrib.compute.mandesero;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.mandesero.internal.BucketId;
import company.vk.edu.distrib.compute.mandesero.internal.BucketLockManager;
import company.vk.edu.distrib.compute.mandesero.internal.BucketRecord;
import company.vk.edu.distrib.compute.mandesero.internal.BucketStorage;
import company.vk.edu.distrib.compute.mandesero.internal.HashResolver;
import company.vk.edu.distrib.compute.mandesero.internal.PathResolver;
import company.vk.edu.distrib.compute.mandesero.internal.RecordCodec;

import java.io.IOException;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

public class FSDao implements Dao<byte[]> {

    private static final Path DEFAULT_ROOT_DIR = Path.of(".data");

    private final HashResolver hashResolver;
    private final PathResolver pathResolver;
    private final BucketLockManager lockManager;
    private final BucketStorage bucketStorage;
    private final AtomicBoolean closed;

    public FSDao() {
        this(DEFAULT_ROOT_DIR);
    }

    public FSDao(Path rootDir) {
        validateRootDir(rootDir);

        this.hashResolver = new HashResolver();
        this.pathResolver = new PathResolver(rootDir);
        this.lockManager = new BucketLockManager();
        this.bucketStorage = new BucketStorage(new RecordCodec());
        this.closed = new AtomicBoolean(false);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        ensureOpen();
        validateKey(key);

        BucketId bucketId = hashResolver.resolve(key);
        Lock lock = lockManager.readLock(bucketId);
        lock.lock();
        try {
            Optional<byte[]> value = bucketStorage.find(pathResolver.bucketPath(bucketId), key);
            return value.orElseThrow(() -> new NoSuchElementException("Key not found: " + key));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        ensureOpen();
        validateKey(key);
        validateValue(value);

        BucketId bucketId = hashResolver.resolve(key);
        Lock lock = lockManager.writeLock(bucketId);
        lock.lock();
        try {
            bucketStorage.upsert(
                    pathResolver.bucketPath(bucketId),
                    pathResolver.tempBucketPath(bucketId),
                    new BucketRecord(key, value)
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        ensureOpen();
        validateKey(key);

        BucketId bucketId = hashResolver.resolve(key);
        Lock lock = lockManager.writeLock(bucketId);
        lock.lock();
        try {
            bucketStorage.delete(
                    pathResolver.bucketPath(bucketId),
                    pathResolver.tempBucketPath(bucketId),
                    key
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("DAO is already closed");
        }
    }

    private static void validateRootDir(Path rootDir) {
        if (rootDir == null) {
            throw new IllegalArgumentException("rootDir must not be null");
        }
    }

    private static void validateKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
    }

    private static void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
    }
}
