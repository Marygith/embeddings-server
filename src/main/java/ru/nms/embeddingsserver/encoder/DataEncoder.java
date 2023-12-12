package ru.nms.embeddingsserver.encoder;

import java.io.IOException;
import java.nio.MappedByteBuffer;

public abstract class DataEncoder<T> {

    public abstract void writeData(T data) throws IOException;

    protected abstract void writeBytes(byte[] bytes, int start, int len) throws IOException;

    public abstract MappedByteBuffer getMappedByteBuffer();
}
