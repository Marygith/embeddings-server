package ru.nms.embeddingsserver.decoder;

import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import ru.nms.embeddingslibrary.model.Embedding;

import static ru.nms.embeddingsserver.util.Constants.MAGIC;

@Getter
public class EmbeddingDecoder extends DataDecoder<Embedding> {

    private final ByteBuffer buffer;
    private final byte[] buf = new byte[4];

    public EmbeddingDecoder(ByteBuffer buffer) {
        try {
            this.buffer = buffer;
            if (buffer.capacity() < MAGIC.length) {
                System.out.println("very baad");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void readData(Embedding embedding) throws IOException {

        byte[] magic = new byte[MAGIC.length];
        readBytes(magic, 0, MAGIC.length);
        if (!Arrays.equals(MAGIC, magic))
            throw new RuntimeException("Not a hasd data file.");
        int id = readInt(buffer);
        int amount = readInt(buffer);
        float[][] vectors = new float[amount][embedding.getEmbeddingSize()];
        int size = embedding.getEmbeddingSize();
        for (int i = 0; i < amount; i++) {
            vectors[i] = readFloatArray(size);
        }
        embedding.setEmbedding(vectors);
        embedding.setId(id);
        System.out.println("read embedding with id " + id);
        System.out.println(buffer.capacity() - buffer.position() + " bytes left");
    }

    @Override
    protected void readBytes(byte[] bytes, int start, int length) throws IOException {
        if (length < 0)
            throw new RuntimeException("Malformed data. Length is negative: " + length);
        buffer.get(bytes);
    }

    public int readInt(ByteBuffer buffer) {
        return buffer.getInt();
    }


    private float[] readFloatArray(int size) throws IOException {
        float[] arr = new float[size];
        for (int i = 0; i < size; i++) {
            arr[i] = readFloat(buffer);
        }
        return arr;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

}
