package company.vk.edu.distrib.compute.mandesero.internal;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class BucketStorage {

    private final RecordCodec codec;

    public BucketStorage(RecordCodec codec) {
        validateCodec(codec);

        this.codec = codec;
    }

    public Optional<byte[]> find(Path bucketPath, String key) throws IOException {
        validatePath(bucketPath, "bucketPath");
        validateKey(key);

        if (Files.notExists(bucketPath)) {
            return Optional.empty();
        }

        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(bucketPath)))) {

            while (input.available() > 0) {
                BucketRecord record = codec.read(input);
                if (record.key().equals(key)) {
                    return Optional.of(record.value());
                }
            }
        }

        return Optional.empty();
    }

    public void upsert(Path bucketPath, Path tempPath, BucketRecord record) throws IOException {
        validatePath(bucketPath, "bucketPath");
        validatePath(tempPath, "tempPath");
        validateRecord(record);

        List<BucketRecord> records = readAll(bucketPath);

        boolean updated = false;
        List<BucketRecord> rewritten = new ArrayList<>(records.size() + 1);

        for (BucketRecord existing : records) {
            if (existing.key().equals(record.key())) {
                rewritten.add(record);
                updated = true;
            } else {
                rewritten.add(existing);
            }
        }

        if (!updated) {
            rewritten.add(record);
        }

        rewriteBucket(bucketPath, tempPath, rewritten);
    }

    public boolean delete(Path bucketPath, Path tempPath, String key) throws IOException {
        validatePath(bucketPath, "bucketPath");
        validatePath(tempPath, "tempPath");
        validateKey(key);

        if (Files.notExists(bucketPath)) {
            return false;
        }

        List<BucketRecord> records = readAll(bucketPath);
        List<BucketRecord> rewritten = new ArrayList<>(records.size());

        boolean deleted = false;
        for (BucketRecord record : records) {
            if (record.key().equals(key)) {
                deleted = true;
            } else {
                rewritten.add(record);
            }
        }

        if (!deleted) {
            return false;
        }

        if (rewritten.isEmpty()) {
            Files.deleteIfExists(bucketPath);
            Files.deleteIfExists(tempPath);
            return true;
        }

        rewriteBucket(bucketPath, tempPath, rewritten);
        return true;
    }

    private List<BucketRecord> readAll(Path bucketPath) throws IOException {
        if (Files.notExists(bucketPath)) {
            return List.of();
        }

        List<BucketRecord> records = new ArrayList<>();

        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(bucketPath)))) {

            while (input.available() > 0) {
                records.add(codec.read(input));
            }
        }

        return records;
    }

    private void rewriteBucket(Path bucketPath, Path tempPath, List<BucketRecord> records) throws IOException {
        ensureParentDirectoryExists(bucketPath);

        try {
            writeAll(tempPath, records);
            replaceAtomically(tempPath, bucketPath);
        } catch (IOException e) {
            Files.deleteIfExists(tempPath);
            throw e;
        }
    }

    private void writeAll(Path tempPath, List<BucketRecord> records) throws IOException {
        try (DataOutputStream output = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(tempPath)))) {

            for (BucketRecord record : records) {
                codec.write(output, record);
            }
        }
    }

    private static void replaceAtomically(Path tempPath, Path bucketPath) throws IOException {
        Files.move(
                tempPath,
                bucketPath,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );
    }

    private static void ensureParentDirectoryExists(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    private static void validateCodec(RecordCodec codec) {
        if (codec == null) {
            throw new IllegalArgumentException("codec must not be null");
        }
    }

    private static void validatePath(Path path, String name) {
        if (path == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }

    private static void validateRecord(BucketRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("record must not be null");
        }
    }

    private static void validateKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
    }
}
