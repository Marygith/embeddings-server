package ru.nms.embeddingsserver.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.nms.embeddingslibrary.model.Embedding;
import ru.nms.embeddingsserver.exception.HashNotFoundException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class RetrievalService {

    @Value("${zookeeper.worker.service.instance.name}")
    private String instanceName;

    private final RegisterService registerService;
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
            throw new RuntimeException(e);
        }
    }


    public byte[] getEmbeddingsAsBytesByHash(int hash) {

        EmbeddingService es = hashToEmbeddingsMap.get(hash);
        if (es == null) {
            throw new HashNotFoundException();
        }
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
        registerService.getInstance().getPayload().getVirtualNodeHashes().remove(Integer.valueOf(hash));
        es.close();
    }

    public void putEmbeddingsByHashFromByteBuffer(int hash, ByteBuffer embeddings) {

        String dirPath = PATH_TO_EMBEDDINGS_DIRECTORY + instanceName + "\\hash_" + hash;
        new File(dirPath).mkdirs();

        EmbeddingService es = hashToEmbeddingsMap.getOrDefault(hash, new EmbeddingService(dirPath + "\\meta.txt", dirPath + "\\embeddings.hasd", dirPath + "\\positions.hasd"));
        try {
            List<Embedding> embeddingList = es.readEmbeddingsFromByteBuffer(embeddings);
            es.putEmbeddingsToFile(embeddingList);

            hashToEmbeddingsMap.put(hash, es);
            registerService.getInstance().getPayload().getVirtualNodeHashes().add(hash);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
