package ru.nms.embeddingsserver.util;


import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;
import ru.nms.embeddingsserver.model.Embedding;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.nms.embeddingsserver.util.Constants.EMBEDDINGS_AMOUNT;
import static ru.nms.embeddingsserver.util.Constants.EMBEDDING_SIZE;

@Getter
@Setter
@Component
public class EmbeddingGenerator {

    private int randomSeedCounter = 0;

    private float[][] embeddingAsArray;

    private int id;

    private long avroId;

    private final Random rand = new Random(312);

    private final List<List<Float>> embeddingAsList = new ArrayList<>();

    private AtomicInteger idCounter = new AtomicInteger();

    public EmbeddingGenerator() {
        embeddingAsArray = new float[EMBEDDINGS_AMOUNT][EMBEDDING_SIZE];
        for (int i = 0; i < EMBEDDINGS_AMOUNT; i++) {
            for (int k = 0; k < EMBEDDING_SIZE; k++) {
                embeddingAsArray[i][k] = Float.float16ToFloat((short) rand.nextInt());
            }
        }

        embeddingAsList.clear();
        for (int i = 0; i < EMBEDDINGS_AMOUNT; i++) {
            embeddingAsList.add(new ArrayList<>());
            for (int k = 0; k < EMBEDDING_SIZE; k++) {
                embeddingAsList.get(i).add(embeddingAsArray[i][k]);
            }
        }
    }
    public void generateData() {
//        generateEmbedding();
//        generateEmbeddingAsList();
        generateId();
//        generateAvroId();
    }

    private void generateEmbedding() {
        embeddingAsArray = new float[EMBEDDINGS_AMOUNT][EMBEDDING_SIZE];
        for (int i = 0; i < EMBEDDINGS_AMOUNT; i++) {
            for (int k = 0; k < EMBEDDING_SIZE; k++) {
                embeddingAsArray[i][k] = Float.float16ToFloat((short) rand.nextInt());
            }
        }
    }

    private void generateId() {
        id = idCounter.incrementAndGet();
    }

    private void generateAvroId() {
        avroId =  UUID.randomUUID().getMostSignificantBits();
    }

    private void generateEmbeddingAsList() {
        embeddingAsList.clear();
        for (int i = 0; i < EMBEDDINGS_AMOUNT; i++) {
            embeddingAsList.add(new ArrayList<>());
            for (int k = 0; k < EMBEDDING_SIZE; k++) {
//                embeddingAsList.get(i).add(rand.nextFloat());
                embeddingAsList.get(i).add(embeddingAsArray[i][k]);
            }
        }
    }

    public List<Embedding> generateNEmbeddings(int n, int startId) {
//        generateData();
        idCounter.set(startId);
        List<Embedding> embeddings = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            generateData();
            embeddings.add(new Embedding(getEmbeddingAsArray(), EMBEDDING_SIZE, id));
        }
        return embeddings;
    }



//    private final int embeddingsAMount = 512;

}
