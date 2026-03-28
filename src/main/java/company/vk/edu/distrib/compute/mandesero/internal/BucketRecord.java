package company.vk.edu.distrib.compute.mandesero.internal;

import java.util.Arrays;

public final class BucketRecord {

    private final String recordKey;
    private final byte[] recordValue;

    public BucketRecord(String key, byte[] value) {
        validateKey(key);
        validateValue(value);

        this.recordKey = key;
        this.recordValue = Arrays.copyOf(value, value.length);
    }

    public String key() {
        return recordKey;
    }

    public byte[] value() {
        return Arrays.copyOf(recordValue, recordValue.length);
    }

    @Override
    public String toString() {
        return "BucketRecord{key='" + recordKey + "', valueLength=" + recordValue.length + "}";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        BucketRecord that = (BucketRecord) other;
        return recordKey.equals(that.recordKey) && Arrays.equals(recordValue, that.recordValue);
    }

    @Override
    public int hashCode() {
        int result = recordKey.hashCode();
        result = 31 * result + Arrays.hashCode(recordValue);
        return result;
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
