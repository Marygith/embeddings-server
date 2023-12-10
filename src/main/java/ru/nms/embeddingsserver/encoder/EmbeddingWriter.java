package ru.nms.embeddingsserver.encoder;

import ru.nms.embeddingsserver.model.Embedding;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import static ru.nms.embeddingsserver.util.Constants.MAGIC;


public class EmbeddingWriter extends DataWriter<Embedding> {


    @Override
    public void create(MappedByteBuffer buffer) throws IOException {
//        FileOutputStream fos = new FileOutputStream(file);
        dataEncoder = new EmbeddingEncoder(buffer);
//        dataEncoder.writeBytes(MAGIC, 0, 4);
    }

    @Override
    public void addData(Embedding data) throws IOException {
        dataEncoder.writeData(data);
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer(){
        return dataEncoder.getMappedByteBuffer();
    }

//    @Override
//    public void close() throws IOException {
//        dataEncoder.close();
//    }

}
