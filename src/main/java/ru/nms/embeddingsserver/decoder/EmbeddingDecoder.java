package ru.nms.embeddingsserver.decoder;

import lombok.Getter;
import ru.nms.embeddingsserver.model.Embedding;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

import static ru.nms.embeddingsserver.util.Constants.MAGIC;

@Getter
public class EmbeddingDecoder extends DataDecoder<Embedding> {

    private final MappedByteBuffer buffer;
    private final byte[] buf = new byte[4];

    public EmbeddingDecoder(MappedByteBuffer buffer) {
        try {
            this.buffer = buffer;
            if (buffer.capacity() < MAGIC.length) {
                System.out.println("very baad");
            }
      /*      byte[] magic = new byte[MAGIC.length];
            readBytes(magic, 0, MAGIC.length);
            if (!Arrays.equals(MAGIC, magic))
                throw new RuntimeException("Not a hasd data file.");*/
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
//            if (readUnsignedInt(buffer) != 0) throw new RuntimeException();
        }
//        if (readUnsignedInt(buffer) != 0) throw new RuntimeException();
        embedding.setEmbedding(vectors);
        embedding.setId(id);
    }

    @Override
    protected void readBytes(byte[] bytes, int start, int length) throws IOException {
        if (length < 0)
            throw new RuntimeException("Malformed data. Length is negative: " + length);
        buffer.get(bytes);
    }

    public int readInt(MappedByteBuffer buffer) throws IOException {
        return buffer.getInt();
//        int n = decodeInt(buffer);
//        return (n >>> 1) ^ -(n & 1);
    }

    public int readUnsignedInt(MappedByteBuffer buffer) throws IOException {
        return decodeInt(buffer);
    }

    private float[] readFloatArray(int size) throws IOException {
        float[] arr = new float[size];
        for (int i = 0; i < size; i++) {
            arr[i] = readFloat(buffer);
        }
        return arr;
    }

    @Override
    public MappedByteBuffer getBuffer() {
        return buffer;
    }

//    @Override
//    public void close() throws IOException {
//        buffer.close();
//    }

//    private String readString() throws IOException {
//        int length = readUnsignedInt(is);
//        byte[] arr = is.readNBytes(length);
//        return new String(arr);
//    }
}
