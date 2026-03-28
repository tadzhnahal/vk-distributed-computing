package company.vk.edu.distrib.compute.mandesero.internal;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"PMD.UnitTestAssertionsShouldIncludeMessage", "PMD.UnitTestContainsTooManyAsserts"})
class RecordCodecTest {

    @Test
    void writeAndReadRecord() throws IOException {
        RecordCodec codec = new RecordCodec();
        BucketRecord record = new BucketRecord("key", new byte[]{1, 2, 3});
        BucketRecord restored = roundTrip(codec, record);

        assertEquals(record, restored);
    }

    @Test
    void writeAndReadRecordWithEmptyValue() throws IOException {
        RecordCodec codec = new RecordCodec();
        BucketRecord record = new BucketRecord("key", new byte[0]);
        BucketRecord restored = roundTrip(codec, record);

        assertEquals(record, restored);
    }

    @Test
    void customCharset() throws IOException {
        BucketRecord record = new BucketRecord("ключ", new byte[]{1, 2, 3});
        RecordCodec codec = new RecordCodec(StandardCharsets.UTF_16BE);

        BucketRecord restored = roundTrip(codec, record);

        assertEquals(record, restored);
    }

    @Test
    void invalidArguments() {
        RecordCodec codec = new RecordCodec();
        DataOutputStream output = new DataOutputStream(new ByteArrayOutputStream());

        assertThrows(IllegalArgumentException.class, () -> new RecordCodec(null));
        assertThrows(IllegalArgumentException.class, ()
                -> codec.write(null, new BucketRecord("key", new byte[]{1})));
        assertThrows(IllegalArgumentException.class, () -> codec.write(output, null));
        assertThrows(IllegalArgumentException.class, () -> codec.read(null));
    }

    @Test
    void negativeLengths() throws IOException {
        RecordCodec codec = new RecordCodec();

        assertThrows(IOException.class, () -> codec.read(inputOf(-1, 3, new byte[]{1, 2, 3})));
        assertThrows(IOException.class, () -> codec.read(inputOf(3, -1, new byte[]{1, 2, 3})));
    }

    @Test
    void truncatedInput() {
        RecordCodec codec = new RecordCodec();

        assertThrows(IOException.class, ()
                -> codec.read(new DataInputStream(new ByteArrayInputStream(new byte[]{0, 0, 0, 3}))));
        assertThrows(IOException.class, () -> codec.read(inputOf(4, 2, new byte[]{'k', 'e'})));
        assertThrows(IOException.class, () -> codec.read(inputOf(3, 4, new byte[]{'k', 'e', 'y', 1, 2})));
    }

    private static BucketRecord roundTrip(RecordCodec codec, BucketRecord record) throws IOException {
        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteArrayOutput);
        codec.write(output, record);

        ByteArrayInputStream byteArrayInput = new ByteArrayInputStream(byteArrayOutput.toByteArray());
        DataInputStream input = new DataInputStream(byteArrayInput);
        return codec.read(input);
    }

    private static DataInputStream inputOf(int keyLength, int valueLength, byte[] body) throws IOException {
        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteArrayOutput);
        output.writeInt(keyLength);
        output.writeInt(valueLength);
        output.write(body);
        output.flush();

        return new DataInputStream(new ByteArrayInputStream(byteArrayOutput.toByteArray()));
    }
}
