package ru.nms.embeddingsserver.encoder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

public abstract class DataWriter<T> {

//    protected FileOutputStream fileOutputStream;
    protected DataEncoder<T> dataEncoder;
    public abstract void create(MappedByteBuffer buffer) throws IOException;

    public abstract void addData(T data) throws IOException;

    public abstract MappedByteBuffer getMappedByteBuffer();

//    public abstract void close() throws IOException;


}
