package ru.nms.embeddingsserver.encoder;

import ru.nms.embeddingslibrary.model.Embedding;

import java.nio.MappedByteBuffer;

import static ru.nms.embeddingsserver.util.Constants.MAGIC;

public class EmbeddingEncoder extends DataEncoder<Embedding> {

    private final MappedByteBuffer mappedByteBuffer;

    public EmbeddingEncoder(MappedByteBuffer mappedByteBuffer) {
        this.mappedByteBuffer = mappedByteBuffer;
    }


    @Override
    public void writeData(Embedding embedding) {
        writeBytes(MAGIC, 0, 4);
        int amount = embedding.getEmbedding().length;
        float[][] vectors = embedding.getEmbedding();
        writeInt(embedding.getId());
        writeInt(amount);
        int size = embedding.getEmbeddingSize();
        for (int i = 0; i < amount; i++) {
            writeFloatArray(vectors[i], size);
        }
    }

    protected void writeFloat(float data) {
        mappedByteBuffer.putShort(Float.floatToFloat16(data));
    }


    public void writeInt(int data) {
        mappedByteBuffer.putInt(data);

    }

    @Override
    public void writeBytes(byte[] bytes, int start, int len) {
        mappedByteBuffer.put(bytes);
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }


    private void writeFloatArray(float[] arr, int len) {
        for (int i = 0; i < len; i++) {
            writeFloat(arr[i]);
        }
    }
}
