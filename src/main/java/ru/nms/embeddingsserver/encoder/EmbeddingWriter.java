package ru.nms.embeddingsserver.encoder;

import java.io.IOException;
import java.nio.MappedByteBuffer;

import ru.nms.embeddingslibrary.model.Embedding;

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
    public MappedByteBuffer getMappedByteBuffer() {
        return dataEncoder.getMappedByteBuffer();
    }

}
