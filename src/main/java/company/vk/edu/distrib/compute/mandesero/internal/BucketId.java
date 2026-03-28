package company.vk.edu.distrib.compute.mandesero.internal;

import java.util.Locale;
import java.util.regex.Pattern;

public final class BucketId {
    private static final Pattern HEX_HASH_PATTERN = Pattern.compile("[0-9a-fA-F]+");

    private final String hexHash;
    private final String primaryDirectory;
    private final String secondaryDirectory;

    public BucketId(String hexHash) {
        validateHexHash(hexHash);

        this.hexHash = hexHash.toLowerCase(Locale.ROOT);
        this.primaryDirectory = this.hexHash.substring(0, 2);
        this.secondaryDirectory = this.hexHash.substring(2, 4);
    }

    public String hex() {
        return hexHash;
    }

    public String dir1() {
        return primaryDirectory;
    }

    public String dir2() {
        return secondaryDirectory;
    }

    public String fileName() {
        return hexHash;
    }

    @Override
    public String toString() {
        return hexHash;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        BucketId that = (BucketId) other;
        return hexHash.equals(that.hexHash);
    }

    @Override
    public int hashCode() {
        return hexHash.hashCode();
    }

    private static void validateHexHash(String hexHash) {
        if (hexHash == null || hexHash.length() < 4 || !HEX_HASH_PATTERN.matcher(hexHash).matches()) {
            throw new IllegalArgumentException("hexHash must contain at least 4 hexadecimal characters");
        }
    }
}
