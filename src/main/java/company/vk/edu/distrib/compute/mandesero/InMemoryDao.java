package company.vk.edu.distrib.compute.mandesero;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryDao implements Dao<byte[]> {
    private final ConcurrentMap<String, byte[]> storage;

    public InMemoryDao() {
        this.storage = new ConcurrentHashMap<>();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);

        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("No value for key: " + key);
        }
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        validateValue(value);

        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);

        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

    private void validateKey(String key) {
        if (Objects.isNull(key) || key.isBlank()) {
            throw new IllegalArgumentException("Key must not be null or blank");
        }
    }

    private static void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
    }
}
