package company.vk.edu.distrib.compute.mandesero.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class RecordCodec {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private final Charset charset;

    public RecordCodec() {
        this.charset = DEFAULT_CHARSET;
    }

    public RecordCodec(Charset charset) {
        validateCharset(charset);

        this.charset = charset;
    }

    public void write(DataOutput output, BucketRecord record) throws IOException {
        validateOutput(output);
        validateRecord(record);

        byte[] keyBytes = record.key().getBytes(charset);
        byte[] valueBytes = record.value();

        validateLength(keyBytes.length, "key length");
        validateLength(valueBytes.length, "value length");

        output.writeInt(keyBytes.length);
        output.writeInt(valueBytes.length);
        output.write(keyBytes);
        output.write(valueBytes);
    }

    public BucketRecord read(DataInput input) throws IOException {
        validateInput(input);

        int keyLength;
        int valueLength;

        try {
            keyLength = input.readInt();
            valueLength = input.readInt();
        } catch (EOFException e) {
            throw new IOException("Unexpected end of input while reading record header", e);
        }

        validateStoredLength(keyLength, "key length");
        validateStoredLength(valueLength, "value length");

        byte[] keyBytes = new byte[keyLength];
        byte[] valueBytes = new byte[valueLength];

        try {
            input.readFully(keyBytes);
            input.readFully(valueBytes);
        } catch (EOFException e) {
            throw new IOException("Unexpected end of input while reading record body", e);
        }

        String key = new String(keyBytes, charset);
        return new BucketRecord(key, valueBytes);
    }

    private static void validateCharset(Charset charset) {
        if (charset == null) {
            throw new IllegalArgumentException("charset must not be null");
        }
    }

    private static void validateOutput(DataOutput output) {
        if (output == null) {
            throw new IllegalArgumentException("output must not be null");
        }
    }

    private static void validateInput(DataInput input) {
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
    }

    private static void validateRecord(BucketRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("record must not be null");
        }
    }

    private static void validateLength(int length, String fieldName) {
        if (length < 0) {
            throw new IllegalArgumentException(fieldName + " must not be negative");
        }
    }

    private static void validateStoredLength(int length, String fieldName) throws IOException {
        if (length < 0) {
            throw new IOException("Malformed record: " + fieldName + " must not be negative");
        }
    }
}
