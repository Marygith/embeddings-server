package ru.nms.embeddingsserver.util;


import lombok.Getter;
import ru.nms.embeddingsserver.decoder.DataReader;
import ru.nms.embeddingsserver.decoder.EmbeddingReader;
import ru.nms.embeddingsserver.encoder.DataWriter;
import ru.nms.embeddingsserver.encoder.EmbeddingWriter;
import ru.nms.embeddingsserver.model.Embedding;
import ru.nms.embeddingsserver.service.MemoryMapper;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Getter
public class Example {

    private static final List<Embedding> embeddings = new ArrayList<>();

    private static final EmbeddingGenerator generator = new EmbeddingGenerator();

    private static final int embeddingListSize = 50;
    public static long getTimeOfReading(MappedByteBuffer buffer) {
        try {
            long start = System.currentTimeMillis();
            DataReader<Embedding> reader = new EmbeddingReader(buffer);
            Embedding embedding = new Embedding(null, Constants.EMBEDDING_SIZE, generator.getId());

            int counter = 0;
            while (reader.hasNext() && counter < embeddingListSize) {
                reader.readData(embedding);
                if (!embedding.equals(embeddings.get(counter))) {
                    return -1;
                }
                counter++;
            }
            return System.currentTimeMillis() - start;
        } catch (IOException e) {
            return -1;
        }

    }

    public static long getTimeOfWriting(MemoryMapper mapper, int embeddingsAmount, String fileName) {
        try {
//            generator.setEmbeddingSize(embeddingsAmount);
            populateEmbeddings();
            long start = System.currentTimeMillis();
            DataWriter<Embedding> writer = new EmbeddingWriter();

            writer.create(mapper.getMappedByteBuffer());
            for (Embedding e : embeddings) {
                writer.addData(e);
            }
            writer.getMappedByteBuffer().load();
//            byte[] bytes = writer.getBos().toByteArray();
//            mapper.initMapperForWriting(fileName, 0, bytes.length);
//            mapper.getMappedByteBuffer().put(bytes);
//            mapper.getMappedByteBuffer().load();
//            buffer.force(0, buffer.position());
            return System.currentTimeMillis() - start;
        } catch (IOException e) {
            return -1;
        }
    }

    private static void populateEmbeddings() {
        for (int i = 0; i < embeddingListSize; i++) {
            generator.generateData();
            embeddings.add(new Embedding(generator.getEmbeddingAsArray(), Constants.EMBEDDING_SIZE, generator.getId()));
        }
    }
}




/////храним файлы с оффсетами
//id :
//    block number
//    offset in block
//ids are getting sorted?
