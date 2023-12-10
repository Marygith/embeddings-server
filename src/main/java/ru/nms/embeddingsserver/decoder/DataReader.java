package ru.nms.embeddingsserver.decoder;



import java.io.IOException;

public abstract class DataReader<T> {

    protected DataDecoder<T> dataDecoder;

    public abstract void readData(T data) throws IOException;

    public abstract boolean hasNext() throws IOException;


//    public abstract void close() throws IOException;

}
