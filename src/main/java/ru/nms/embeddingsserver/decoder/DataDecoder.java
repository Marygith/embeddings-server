package ru.nms.embeddingsserver.decoder;


import java.io.IOException;
import java.nio.MappedByteBuffer;

import static java.lang.Float.float16ToFloat;

public abstract class DataDecoder<T> {
    protected abstract void readData(T data) throws IOException;

    public float readFloat(MappedByteBuffer buffer) {
        return float16ToFloat(buffer.getShort());
    }

    protected abstract void readBytes(byte[] bytes, int start, int len) throws IOException;

    public abstract MappedByteBuffer getBuffer();
}
