package ru.nms.embeddingsserver.decoder;

import java.io.IOException;
import java.nio.ByteBuffer;

import ru.nms.embeddingslibrary.model.Embedding;

public class EmbeddingReader extends DataReader<Embedding> {


    public EmbeddingReader(ByteBuffer buffer) {
        dataDecoder = new EmbeddingDecoder(buffer);
    }

    @Override
    public void readData(Embedding data) throws IOException {
        dataDecoder.readData(data);
    }

    @Override
    public boolean hasNext() throws IOException {
        return dataDecoder.getBuffer().position() < dataDecoder.getBuffer().capacity();
    }

}
