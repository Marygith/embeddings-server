package ru.nms.embeddingsserver.encoder;

import java.io.IOException;
import java.nio.MappedByteBuffer;

public abstract class DataWriter<T> {

    protected DataEncoder<T> dataEncoder;

    public abstract void create(MappedByteBuffer buffer) throws IOException;

    public abstract void addData(T data) throws IOException;

    public abstract MappedByteBuffer getMappedByteBuffer();

}
