package company.vk.edu.distrib.compute.mandesero.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"PMD.UnitTestAssertionsShouldIncludeMessage", "PMD.UnitTestContainsTooManyAsserts"})
class BucketIdTest {
    private static final String MIXED_CASE_HEX = "AbCd1234";
    private static final String NORMALIZED_HEX = "abcd1234";
    private static final String BUCKET_HEX = "abcdef12";

    @Test
    void hex() {
        BucketId bucketId = new BucketId(MIXED_CASE_HEX);

        assertEquals(NORMALIZED_HEX, bucketId.hex());
    }

    @Test
    void dir1() {
        BucketId bucketId = new BucketId(BUCKET_HEX);

        assertEquals("ab", bucketId.dir1());
    }

    @Test
    void dir2() {
        BucketId bucketId = new BucketId(BUCKET_HEX);

        assertEquals("cd", bucketId.dir2());
    }

    @Test
    void filename() {
        BucketId bucketId = new BucketId(MIXED_CASE_HEX);

        assertEquals(NORMALIZED_HEX, bucketId.fileName());
    }

    @Test
    void convertToString() {
        BucketId bucketId = new BucketId(MIXED_CASE_HEX);

        assertEquals(NORMALIZED_HEX, bucketId.toString());
    }

    @Test
    void equality() {
        BucketId left = new BucketId(MIXED_CASE_HEX);
        BucketId right = new BucketId(NORMALIZED_HEX);

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());

        left = new BucketId(NORMALIZED_HEX);
        right = new BucketId("abcd1235");

        assertNotEquals(left, right);
        assertNotEquals(null, right);
        assertNotEquals(NORMALIZED_HEX, right);
    }

    @Test
    void shouldNotBeEqualToNull() {
        BucketId bucketId = new BucketId(NORMALIZED_HEX);

        assertNotEquals(null, bucketId);
    }

    @Test
    void init() {
        assertThrows(IllegalArgumentException.class, () -> new BucketId(null));
        assertThrows(IllegalArgumentException.class, () -> new BucketId(""));
        assertThrows(IllegalArgumentException.class, () -> new BucketId("abc"));
        assertThrows(IllegalArgumentException.class, () -> new BucketId("abcz1234"));
    }
}
