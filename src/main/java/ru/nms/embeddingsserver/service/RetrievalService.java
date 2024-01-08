package ru.nms.embeddingsserver.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.nms.embeddingslibrary.model.Embedding;
import ru.nms.embeddingsserver.exception.EmbeddingNotFoundException;
import ru.nms.embeddingsserver.exception.HashNotFoundException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class RetrievalService {

    @Value("${zookeeper.worker.service.instance.name}")
    private String instanceName;


    private static final String PATH_TO_EMBEDDINGS_DIRECTORY = "D:\\embeddings\\";


    private final Map<Integer, EmbeddingService> hashToEmbeddingsMap = new HashMap<>();

    public Embedding getEmbeddingById(int hash, int id) {
        try {
            EmbeddingService es = hashToEmbeddingsMap.get(hash);
            if (es == null) {
                throw new HashNotFoundException();
            }
            return es.findEmbeddingById(id);
        } catch (IOException e) {
            throw new EmbeddingNotFoundException();
        }
    }


/*    public List<Embedding> getEmbeddingsByHash(int hash) {
        EmbeddingService es = hashToEmbeddingsMap.get(hash);
        List<Embedding> embeddings;
        try {
            embeddings = es.getAllEmbeddings();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return embeddings;
    }*/

    public byte[] getEmbeddingsAsBytesByHash(int hash) {
        EmbeddingService es = hashToEmbeddingsMap.get(hash);
        byte[] embeddings;
        try {
            embeddings = es.getAllEmbeddingsAsBytes();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return embeddings;
    }

    public void deleteEmbeddingsByHash(int hash) {
        EmbeddingService es = hashToEmbeddingsMap.remove(hash);
        try {
            es.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void putEmbeddingsByHashFromByteBuffer(int hash, ByteBuffer embeddings) {
        String dirPath = PATH_TO_EMBEDDINGS_DIRECTORY + instanceName + "\\hash_" + hash;
        new File(dirPath).mkdirs();

        EmbeddingService es = hashToEmbeddingsMap.getOrDefault(hash, new EmbeddingService(dirPath + "\\meta.txt", dirPath + "\\embeddings.hasd", dirPath + "\\positions.hasd"));
        try {
            List<Embedding> embeddingList = es.readEmbeddingsFromByteBuffer(embeddings);
            log.info("Read " + embeddingList.size() + " embeddings");
            es.putEmbeddingsToFile(embeddingList);
            log.info("put these embeddings to file");

            hashToEmbeddingsMap.put(hash, es);
            log.info("Put new embeddings with hash " + hash + ", map is now has keys " + Arrays.toString(hashToEmbeddingsMap.keySet().toArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


/*    public void addEmbeddingsByHash(int hash, List<Embedding> embeddings) {

        EmbeddingService es = hashToEmbeddingsMap.getOrDefault(hash, new EmbeddingService());
        int index = 0;
        List<List<Embedding>> customEmbeddings = new ArrayList<>();

        while (index < embeddings.size()) {
            customEmbeddings.add(new ArrayList<>(embeddings.subList(index, (int) Math.min(index + BLOCK_SIZE, embeddings.size()))));
            index += BLOCK_SIZE;
        }
        for (List<Embedding> e : customEmbeddings) {
            try {
                es.putEmbeddingsToFile(e);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }*/
}
