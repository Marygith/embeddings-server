package ru.nms.embeddingsserver.util;

public class Constants {
    public static final String PATH_TO_EMBEDDINGS_DIRECTORY = "D:\\embeddings\\";

    public static final int EMBEDDING_SIZE = 768;
    public static final int EMBEDDINGS_AMOUNT = 512;

    public static final Long EMBEDDING_SIZE_IN_BYTES = 786_444L;
    public static final String POSITION = "position";
    public static final String BLOCK_NUMBER = "block_number";
    public static final String AMOUNT_OF_EMBEDDINGS_IN_BLOCK = "amount_of_embeddings_in_block";
    public static final Long BLOCK_SIZE = 300L;

    // 768 * 2 = 1 536 bytes (one embedding)
    //1 536 * 512 = 786 432 bytes (768 kilobytes)
    //786 432 + 4(hasd) = 786 436 bytes
    //786 436 + 8(id + amount) = 786 444
    // ~ 300 embeddings in one block

    public static final String PATH_TO_META_FILE = "C:\\Users\\maria\\dev\\embeddings\\meta.txt";
    public static final String PATH_TO_EMBEDDINGS_FILE = "C:\\Users\\maria\\dev\\embeddings\\embeddings.hasd";

    public static final String PATH_TO_POSITIONS = "C:\\Users\\maria\\dev\\embeddings\\positions.hasd";

    public static final byte[] MAGIC = new byte[]{(byte) 'H', (byte) 'a', (byte) 's', (byte) 'd'};

}
