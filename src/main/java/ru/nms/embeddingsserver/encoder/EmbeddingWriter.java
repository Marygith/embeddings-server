package ru.nms.embeddingsserver.encoder;

import ru.nms.embeddingsserver.model.Embedding;

import java.io.IOException;
import java.nio.MappedByteBuffer;


public class EmbeddingWriter extends DataWriter<Embedding> {


    @Override
    public void create(MappedByteBuffer buffer) {
        dataEncoder = new EmbeddingEncoder(buffer);
    }

    @Override
    public void addData(Embedding data) throws IOException {
        dataEncoder.writeData(data);
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer(){
        return dataEncoder.getMappedByteBuffer();
    }


}
