package ru.nms.embeddingsserver.decoder;



import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import static java.lang.Float.float16ToFloat;

public abstract class DataDecoder<T> {
    protected abstract void readData(T data) throws IOException;

    protected  int decodeInt(MappedByteBuffer mappedByteBuffer) throws IOException {
        int n = 0;
        int b;
        int shift = 0;
        do {
            b = mappedByteBuffer.getInt();
            if (b >= 0) {
                n |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return n;
                }
            } else {
                throw new EOFException();
            }
            shift += 7;
        } while (shift < 32);
        throw new RuntimeException("Invalid int encoding");

    };

    public float readFloat(MappedByteBuffer buffer) throws IOException {
        return float16ToFloat(buffer.getShort());

    }

    protected abstract void readBytes(byte[] bytes, int start, int len) throws IOException;

    public abstract MappedByteBuffer getBuffer();

//    public abstract void close() throws IOException;
}
