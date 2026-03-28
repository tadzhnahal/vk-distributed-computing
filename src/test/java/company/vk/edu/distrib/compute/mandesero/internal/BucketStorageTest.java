package company.vk.edu.distrib.compute.mandesero.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"PMD.UnitTestAssertionsShouldIncludeMessage", "PMD.UnitTestContainsTooManyAsserts"})
class BucketStorageTest {
    private static final String BUCKET_PATH = "ab/cd/bucket";
    private static final String TEMP_BUCKET_PATH = "ab/cd/bucket.tmp";
    private static final String KEY = "key";
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";

    @TempDir
    Path tempDir;

    @Test
    void find() throws IOException {
        BucketStorage storage = new BucketStorage(new RecordCodec());
        Path bucketPath = tempDir.resolve(BUCKET_PATH);
        Path tempPath = tempDir.resolve(TEMP_BUCKET_PATH);

        storage.upsert(bucketPath, tempPath, new BucketRecord(KEY, new byte[]{1, 2, 3}));

        Optional<byte[]> value = storage.find(bucketPath, KEY);

        assertTrue(value.isPresent());
        assertArrayEquals(new byte[]{1, 2, 3}, value.get());

        value = storage.find(bucketPath, "another_key");
        assertTrue(value.isEmpty());
    }

    @Test
    void upsert() throws IOException {
        BucketStorage storage = new BucketStorage(new RecordCodec());
        Path bucketPath = tempDir.resolve(BUCKET_PATH);
        Path tempPath = tempDir.resolve(TEMP_BUCKET_PATH);

        storage.upsert(bucketPath, tempPath, new BucketRecord(KEY, new byte[]{1, 2, 3}));
        storage.upsert(bucketPath, tempPath, new BucketRecord(KEY, new byte[]{4, 5, 6}));

        Optional<byte[]> value = storage.find(bucketPath, KEY);

        assertTrue(value.isPresent());
        assertArrayEquals(new byte[]{4, 5, 6}, value.get());
    }

    @Test
    void delete() throws IOException {
        BucketStorage storage = new BucketStorage(new RecordCodec());
        Path bucketPath = tempDir.resolve(BUCKET_PATH);
        Path tempPath = tempDir.resolve(TEMP_BUCKET_PATH);

        storage.upsert(bucketPath, tempPath, new BucketRecord(KEY1, new byte[]{1}));
        storage.upsert(bucketPath, tempPath, new BucketRecord(KEY2, new byte[]{2}));

        boolean deleted = storage.delete(bucketPath, tempPath, KEY1);

        assertTrue(deleted);
        assertTrue(storage.find(bucketPath, KEY1).isEmpty());
        assertArrayEquals(new byte[]{2}, storage.find(bucketPath, KEY2).orElseThrow());
    }

    @Test
    void deleteMissingKey() throws IOException {
        BucketStorage storage = new BucketStorage(new RecordCodec());
        Path bucketPath = tempDir.resolve(BUCKET_PATH);
        Path tempPath = tempDir.resolve(TEMP_BUCKET_PATH);

        boolean deleted = storage.delete(bucketPath, tempPath, "missing");

        assertFalse(deleted);
    }

    @Test
    void verify() {
        BucketStorage storage = new BucketStorage(new RecordCodec());
        Path bucketPath = tempDir.resolve("bucket");

        assertThrows(IllegalArgumentException.class, () -> new BucketStorage(null));

        assertThrows(IllegalArgumentException.class, () -> storage.find(null, KEY));
        assertThrows(IllegalArgumentException.class, () -> storage.find(bucketPath, null));

        BucketRecord record = new BucketRecord(KEY, new byte[]{1});
        Path tempPath = tempDir.resolve("bucket.tmp");

        assertThrows(IllegalArgumentException.class, () -> storage.upsert(null, tempPath, record));
        assertThrows(IllegalArgumentException.class, () -> storage.upsert(bucketPath, null, record));
        assertThrows(IllegalArgumentException.class, () -> storage.upsert(bucketPath, tempPath, null));

        assertThrows(IllegalArgumentException.class, () -> storage.delete(null, tempPath, KEY));
        assertThrows(IllegalArgumentException.class, () -> storage.delete(bucketPath, null, KEY));
        assertThrows(IllegalArgumentException.class, () -> storage.delete(bucketPath, tempPath, null));
    }

    @Test
    void cleanUpTempFileAfterRewrite() throws IOException {
        BucketStorage storage = new BucketStorage(new RecordCodec());
        Path bucketPath = tempDir.resolve(BUCKET_PATH);
        Path tempPath = tempDir.resolve(TEMP_BUCKET_PATH);

        storage.upsert(bucketPath, tempPath, new BucketRecord(KEY, new byte[]{1, 2, 3}));

        assertFalse(Files.exists(tempPath));
        assertTrue(Files.exists(bucketPath));
    }

    @Test
    void multipleRecordsInSameBucketAsCollision() throws IOException {
        BucketStorage storage = new BucketStorage(new RecordCodec());
        Path bucketPath = tempDir.resolve("ab/cd/collision-bucket");
        Path tempPath = tempDir.resolve("ab/cd/collision-bucket.tmp");

        storage.upsert(bucketPath, tempPath, new BucketRecord(KEY1, new byte[]{1}));
        storage.upsert(bucketPath, tempPath, new BucketRecord(KEY2, new byte[]{2}));
        storage.upsert(bucketPath, tempPath, new BucketRecord("key3", new byte[]{3}));

        assertArrayEquals(new byte[]{1}, storage.find(bucketPath, KEY1).orElseThrow());
        assertArrayEquals(new byte[]{2}, storage.find(bucketPath, KEY2).orElseThrow());
        assertArrayEquals(new byte[]{3}, storage.find(bucketPath, "key3").orElseThrow());

        assertTrue(storage.delete(bucketPath, tempPath, KEY2));

        assertArrayEquals(new byte[]{1}, storage.find(bucketPath, KEY1).orElseThrow());
        assertTrue(storage.find(bucketPath, KEY2).isEmpty());
        assertArrayEquals(new byte[]{3}, storage.find(bucketPath, "key3").orElseThrow());
    }
}
