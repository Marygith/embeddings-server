package ru.nms.embeddingsserver.encoder;

import ru.nms.embeddingsserver.model.Embedding;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.util.Map;

import static ru.nms.embeddingsserver.util.Constants.MAGIC;

public class EmbeddingEncoder extends DataEncoder<Embedding> {
    private final byte[] buffer = new byte[8];

    private final MappedByteBuffer mappedByteBuffer;
    public EmbeddingEncoder(MappedByteBuffer mappedByteBuffer) {
        this.mappedByteBuffer = mappedByteBuffer;
    }

//    private final ByteArrayOutputStream bos = new ByteArrayOutputStream();

//    private final DataOutputStream dos = new DataOutputStream(bos);

    @Override
    public void writeData(Embedding embedding) throws IOException {
        writeBytes(MAGIC, 0, 4);
        int amount = embedding.getEmbedding().length;
        float[][] vectors = embedding.getEmbedding();
        writeInt(embedding.getId());
        writeInt(amount);
        int size = embedding.getEmbeddingSize();
        for (int i = 0; i < amount; i++) {
            writeFloatArray(vectors[i], size);
//            writeZero();

        }
//        writeZero();
    }

//    @Override
    protected void writeFloat(float data) throws IOException {
//        encodeFloat(data, buffer);
        mappedByteBuffer.putShort(Float.floatToFloat16(data));
//        dos.write(buffer, 0, 2);
//        mappedByteBuffer.put(buffer, 0, 4);
    }

//    @Override
    public void writeInt(int data) throws IOException {
        mappedByteBuffer.putInt(data);
//        int val = (data << 1) ^ (data >> 31);
//        if (valueIsSmall(val)) return;
//        int len = encodeInt(data, buffer);
//        mappedByteBuffer.put(buffer, 0, len);
    }



    public void writeUnsignedInt(int data) throws IOException {
        if (valueIsSmall(data)) return;
        int len = encodeUnsignedInt(data, buffer);
        mappedByteBuffer.put(buffer, 0, len);
//        dos.write(buffer, 0, len);

    }

    private boolean valueIsSmall(int data) throws IOException {
        if ((data & ~0x7F) == 0) {
            mappedByteBuffer.putInt(data);
//            dos.writeInt(data);
            return true;
        } else if ((data & ~0x3FFF) == 0) {
            mappedByteBuffer.putInt(0x80 | data);
            mappedByteBuffer.putInt(data >>> 7);
//            dos.writeInt(0x80 | data);
//            dos.writeInt(data >>> 7);
            return true;
        }
        return false;
    }

//
//    @Override
//    public void close() throws IOException {
//        out.flush();
//        out.close();
//    }

    @Override
    public void writeBytes(byte[] bytes, int start, int len) throws IOException {
//        dos.write(bytes);
        mappedByteBuffer.put(bytes);
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    protected void writeZero() throws IOException {
//        dos.writeInt(0);
        mappedByteBuffer.putInt(0);
    }

    private void writeFloatArray(float[] arr, int len) throws IOException {
        for (int i = 0; i < len; i++) {
            writeFloat(arr[i]);
        }
    }

//    private void writeString(String string) throws IOException {
//        byte[] arr = string.getBytes();
//        writeUnsignedInt(arr.length);
//        out.write(arr);
//    }
}
