package company.vk.edu.distrib.compute.mandesero.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"PMD.UnitTestAssertionsShouldIncludeMessage", "PMD.UnitTestContainsTooManyAsserts"})
class BucketRecordTest {
    private static final String TEST_KEY = "key";
    private static final String ANOTHER_TEST_KEY = "another_key";

    @Test
    void key() {
        BucketRecord record = new BucketRecord(TEST_KEY, new byte[]{1, 2, 3});

        assertEquals(TEST_KEY, record.key());
    }

    @Test
    void value() {
        BucketRecord record = new BucketRecord(TEST_KEY, new byte[]{1, 2, 3});

        assertArrayEquals(new byte[]{1, 2, 3}, record.value());
    }

    @Test
    void immutableConstructor() {
        byte[] source = {1, 2, 3};

        BucketRecord record = new BucketRecord(TEST_KEY, source);
        source[0] = 9;

        assertArrayEquals(new byte[]{1, 2, 3}, record.value());
    }

    @Test
    void immutableValue() {
        BucketRecord record = new BucketRecord(TEST_KEY, new byte[]{1, 2, 3});

        byte[] value = record.value();
        value[0] = 9;

        assertArrayEquals(new byte[]{1, 2, 3}, record.value());
    }

    @Test
    void equality() {
        BucketRecord record1 = new BucketRecord(TEST_KEY, new byte[]{1, 2, 3});
        BucketRecord record2 = new BucketRecord(TEST_KEY, new byte[]{1, 2, 3});
        BucketRecord record3 = new BucketRecord(ANOTHER_TEST_KEY, new byte[]{1, 2, 3});

        assertEquals(record1, record2);
        assertEquals(record1.hashCode(), record2.hashCode());

        assertNotEquals(record1, record3);

        BucketRecord record4 = new BucketRecord(ANOTHER_TEST_KEY, new byte[]{1, 2, 4});
        assertNotEquals(record3, record4);

        assertNotEquals(null, record4);

    }

    @Test
    void validate() {
        assertThrows(IllegalArgumentException.class, () -> new BucketRecord(null, new byte[]{1, 2, 3}));
        assertThrows(IllegalArgumentException.class, () -> new BucketRecord(TEST_KEY, null));

    }

    @Test
    void shouldThrowForNullValue() {
        assertThrows(IllegalArgumentException.class, () -> new BucketRecord(TEST_KEY, null));
    }

    @Test
    void convertToString() {
        BucketRecord record = new BucketRecord(TEST_KEY, new byte[]{1, 2, 3});

        String recordString = record.toString();
        assertTrue(recordString.contains(TEST_KEY));
        assertTrue(recordString.contains("3"));
    }
}
