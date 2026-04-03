package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte [] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
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

        if (value == null) {
            throw new IllegalArgumentException("Value most not be null");
        }

        storage.put(key, Arrays.copyOf(value, value.length));
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
    }


    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }
    }
}


