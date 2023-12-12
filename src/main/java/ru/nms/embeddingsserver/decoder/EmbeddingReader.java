package ru.nms.embeddingsserver.decoder;

import ru.nms.embeddingsserver.model.Embedding;

import java.io.IOException;
import java.nio.MappedByteBuffer;


public class EmbeddingReader extends DataReader<Embedding> {


    public EmbeddingReader(MappedByteBuffer buffer) {
        dataDecoder = new EmbeddingDecoder(buffer);
    }

    @Override
    public void readData(Embedding data) throws IOException {
        dataDecoder.readData(data);
    }

    @Override
    public boolean hasNext() throws IOException {
        return dataDecoder.getBuffer().capacity() > 0;
    }

}
