package ru.nms.embeddingsserver;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.nms.embeddingsserver.exception.EmbeddingNotFoundException;
import ru.nms.embeddingsserver.model.Embedding;
import ru.nms.embeddingsserver.service.EmbeddingService;
import ru.nms.embeddingsserver.util.EmbeddingGenerator;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static ru.nms.embeddingsserver.util.Constants.*;

@SpringBootTest
class EmbeddingsServerApplicationTests {

    @Autowired
    private EmbeddingGenerator generator;

    @Autowired
    private EmbeddingService service;

    private final int testIdFromSecondBatch = 7;
    private final int testIdFromFirstBatch = 3;

    @Test
    void deleteTest() throws IOException {
        assertDoesNotThrow(() ->
                service.findEmbeddingById(testIdFromFirstBatch));
        assertTrue(service.deleteEmbeddingById(testIdFromFirstBatch));
        assertThrows(EmbeddingNotFoundException.class, () ->
                service.findEmbeddingById(testIdFromFirstBatch));
    }

    @Test
    void createAndGetTest() throws IOException {
        assertDoesNotThrow(() ->
                service.findEmbeddingById(testIdFromFirstBatch));
        Embedding embedding = service.findEmbeddingById(1);
        assertEquals(1, embedding.getId());
        assertThrows(EmbeddingNotFoundException.class, () ->
                service.findEmbeddingById(testIdFromSecondBatch));

        //put second batch, with sample id
        List<Embedding> batch2 = generator.generateNEmbeddings(5, 5); //testIdFromSecondBatch is supposed to be here
        service.putEmbeddingsToFile(batch2);
        embedding = service.findEmbeddingById(testIdFromSecondBatch);
        assertEquals(embedding, batch2.get(testIdFromSecondBatch - 6));
    }

    @Test
    void nonStandardBatchSizeCreateTest() throws IOException {

        List<Embedding> batch2 = generator.generateNEmbeddings(3, 5);
        List<Embedding> batch3 = generator.generateNEmbeddings(4, 8);

        service.putEmbeddingsToFile(batch2);
        Embedding embedding = service.findEmbeddingById(6);
        assertEquals(6, embedding.getId());

        service.putEmbeddingsToFile(batch3);
        embedding = service.findEmbeddingById(11);
        assertEquals(11, embedding.getId());

    }

    @Test
    void updateTest() throws IOException {
        assertDoesNotThrow(() ->
                service.findEmbeddingById(testIdFromFirstBatch));

        //update embedding with sample id
        Embedding newEmbedding = generator.generateNEmbeddings(1, 5).get(0);
        assertTrue(service.updateEmbeddingById(testIdFromFirstBatch, newEmbedding));
        Embedding updatedEmbedding = service.findEmbeddingById(testIdFromFirstBatch);
        assertTrue(Arrays.deepEquals(newEmbedding.getEmbedding(), updatedEmbedding.getEmbedding()));

    }

    @BeforeEach
    public void setUp() throws IOException {
        long dirname = System.currentTimeMillis();
        new File(PATH_TO_EMBEDDINGS_DIRECTORY + dirname).mkdirs();
        service.setPathToEmbeddingsFile(PATH_TO_EMBEDDINGS_DIRECTORY + dirname + "\\embeddings.hasd");
        service.setPathToMetaFile(PATH_TO_EMBEDDINGS_DIRECTORY + dirname + "\\meta.txt");
        service.setPathToPositionsFile(PATH_TO_EMBEDDINGS_DIRECTORY + dirname + "\\positions.hasd");
        List<Embedding> batch1 = generator.generateNEmbeddings(5, 0);
        service.putEmbeddingsToFile(batch1);
    }

}
