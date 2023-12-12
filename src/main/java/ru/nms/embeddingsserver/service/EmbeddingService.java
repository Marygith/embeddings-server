package ru.nms.embeddingsserver.service;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Service;
import ru.nms.embeddingsserver.decoder.DataReader;
import ru.nms.embeddingsserver.decoder.EmbeddingReader;
import ru.nms.embeddingsserver.encoder.DataWriter;
import ru.nms.embeddingsserver.encoder.EmbeddingWriter;
import ru.nms.embeddingsserver.exception.EmbeddingNotFoundException;
import ru.nms.embeddingsserver.exception.StorageOutOfSyncException;
import ru.nms.embeddingsserver.model.Embedding;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static ru.nms.embeddingsserver.util.Constants.*;

@Service
@Setter
public class EmbeddingService {
    @Getter
    private MappedByteBuffer positionsBuffer;

    private String pathToMetaFile;

    private String pathToEmbeddingsFile;

    private String pathToPositionsFile;

    List<String> metaKeys = List.of(POSITION, BLOCK_NUMBER, AMOUNT_OF_EMBEDDINGS_IN_BLOCK);

    /**
     * Получение эмбеддинга по id
     *
     * @param id - id документа
     * @return - документ(эмбеддинг)
     * @throws IOException
     */
    public Embedding findEmbeddingById(int id) throws IOException {
        Map<String, Long> meta = getMeta();
        long position = findEmbeddingPosition(id, meta.get(BLOCK_NUMBER)
                * BLOCK_SIZE + meta.get(AMOUNT_OF_EMBEDDINGS_IN_BLOCK));
        if (position == -1) throw new EmbeddingNotFoundException();
        return getEmbeddingByPosition(position, id);
    }


    /**
     * Сохранение эмбеддингов
     *
     * @param embeddings - эмбеддинги
     * @throws IOException
     */
    public void putEmbeddingsToFile(List<Embedding> embeddings) throws IOException {
        initMap(embeddings.size());
        Map<String, Long> meta = getMeta();
        writeEmbeddingsToBlock(embeddings, meta.getOrDefault(POSITION, 0L));
        writeMeta(embeddings, meta);
        positionsBuffer.load();
    }

    /**
     * Удаление эмбеддинга по id.
     * Данное действие не удаляет сам эмбеддинг с диска,
     * а удаляет только информацию о нем из дисковой подсистемы хранения.
     * Для удаления эмбеддинга с диска необходимо провести синхронизацию
     * хранилища с метаданными.
     *
     * @param id - id эмбеддинга
     * @return - было ли удаление успешным
     * @throws IOException
     */
    public boolean deleteEmbeddingById(int id) throws IOException {
        Map<String, Long> meta = getMeta();
        int index = findEmbeddingInd(id, meta.get(BLOCK_NUMBER) * BLOCK_SIZE + meta.get(AMOUNT_OF_EMBEDDINGS_IN_BLOCK));
        if (index == -1) return false;
        deletePositionsInfo(index, meta);
        positionsBuffer.load();
        return true;
    }


    /**
     * Обновление эмбеддинга по id.
     * Сохраняет эмбеддинг вместо старого, с таким же id.
     *
     * @param id        - id эмбеддинга.
     * @param embedding - новое значение
     * @return - было ли обновление успешным
     * @throws IOException
     */
    public boolean updateEmbeddingById(int id, Embedding embedding) throws IOException {
        Map<String, Long> meta = getMeta();
        long position = findEmbeddingPosition(id, meta.get(BLOCK_NUMBER) * BLOCK_SIZE + meta.get(AMOUNT_OF_EMBEDDINGS_IN_BLOCK));
        if (position == 1) return false;
        embedding.setId(id);
        writeEmbeddingsToBlock(List.of(embedding), position);
        return true;
    }

    private void deletePositionsInfo(int position, Map<String, Long> meta) throws IOException {
        positionsBuffer.position(position + 12);

        // Создаем буфер для считывания оставшихся байтов
        byte[] buffer = new byte[(int) (positionsBuffer.capacity() - position - 12)];
        // Считываем оставшиеся байты в буфер
        positionsBuffer.get(buffer);
        // Перемещаем указатель обратно на позицию, с которой нужно начать удаление
        positionsBuffer.position(position);
        // Записываем оставшиеся байты обратно в файл
        positionsBuffer.put(buffer);

        File file = new File(pathToMetaFile);
        List<String> lines = FileUtils.readLines(file, "utf8");
        if (!lines.isEmpty()) lines.set(0, POSITION + " " +
                (meta.getOrDefault(POSITION, 0L) - EMBEDDING_SIZE_IN_BYTES));
        FileUtils.writeLines(file, lines);

    }

    private void writeEmbeddingsToBlock(List<Embedding> embeddings, long position) {
        MemoryMapper mapper = new MemoryMapper();
        mapper.initMapperForWriting(pathToEmbeddingsFile, position,
                embeddings.size() * EMBEDDING_SIZE_IN_BYTES);
        DataWriter<Embedding> writer = new EmbeddingWriter();
        try {
            writer.create(mapper.getMappedByteBuffer());
            for (Embedding e : embeddings) {
                writer.addData(e);
            }
            writer.getMappedByteBuffer().load();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Получение метаинорфмации об уже записанных файлах.
     *
     * @return - мапа с метаинформацией.
     * @throws IOException
     */
    private Map<String, Long> getMeta() throws IOException {
        Map<String, Long> meta = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(pathToMetaFile))) {


            for (String key : metaKeys) {
                String line = reader.readLine();
                if (line == null) break;
                String value = line.substring(line.indexOf(key) + key.length() + 1);
                meta.put(key, Long.parseLong(value));

            }
        } catch (FileNotFoundException e) {
            Path textFilePath = Paths.get(pathToMetaFile);
            if (Files.notExists(textFilePath)) Files.createFile(textFilePath);
            return new HashMap<>();
        }
        return meta;
    }

    /**
     * Запись метаинформации после добавления эмбеддингов.
     * Общая информация(позиция последнего записанного байта, текущий номер блока
     * и количество эмбеддингов в текущем блоке пишется в файл с мета информацией.
     * Метаинформация каждого эмбеддинга(его id, номер его блока и оффсет в этом блоке
     * пишется в отдельный файл с позициями).
     *
     * @param embeddings - добавленные эмбеддинги
     * @param meta       - текущая метаинформация
     * @throws IOException
     */
    private void writeMeta(List<Embedding> embeddings, Map<String, Long> meta) throws IOException {
        File file = new File(pathToMetaFile);
        List<String> lines = FileUtils.readLines(file, "utf8");

        long amountOfEmbeddingsInBlock = meta.getOrDefault(AMOUNT_OF_EMBEDDINGS_IN_BLOCK, 0L);
        long blockNumber = meta.getOrDefault(BLOCK_NUMBER, 0L);
        positionsBuffer.position((int) (blockNumber * BLOCK_SIZE + amountOfEmbeddingsInBlock) * 12);
        for (Embedding embedding : embeddings) {
            if (amountOfEmbeddingsInBlock >= BLOCK_SIZE) {
                amountOfEmbeddingsInBlock = 0;
                blockNumber++;
            }
            positionsBuffer.putInt(embedding.getId());
            positionsBuffer.putInt(Math.toIntExact(amountOfEmbeddingsInBlock++));
            positionsBuffer.putInt(Math.toIntExact(blockNumber));
        }
        if (lines.isEmpty())
            lines.addAll(List.of("", "", "")); //EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + amountOfEmbeddingsInBlock)
        lines.set(0, POSITION + " " + (meta.getOrDefault(POSITION, 0L) + embeddings.size() * EMBEDDING_SIZE_IN_BYTES));
        lines.set(1, BLOCK_NUMBER + " " + blockNumber);
        lines.set(2, AMOUNT_OF_EMBEDDINGS_IN_BLOCK + " " + amountOfEmbeddingsInBlock);
        FileUtils.writeLines(file, lines);
        positionsBuffer.load();
    }


    private long findEmbeddingPosition(Integer embeddingId, long position) throws IOException {
        int left = 0;
        int right = (int) position - 1;
        int id;
        while (right - left > 1) {
            positionsBuffer.position((left + right) / 2 * 12);
            id = positionsBuffer.getInt();
            if (id > embeddingId) {
                right = (left + right) / 2;
                continue;
            }
            if (id < embeddingId) {
                left = (left + right) / 2;
                continue;
            }
            int offset = positionsBuffer.getInt();
            int blockNumber = positionsBuffer.getInt();
            return EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + offset);
        }
        positionsBuffer.position((left + right) / 2 * 12);
        if (positionsBuffer.getInt() == embeddingId) {
            int offset = positionsBuffer.getInt();
            int blockNumber = positionsBuffer.getInt();
            return EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + offset);
        }
        positionsBuffer.position(right * 12);

        if (positionsBuffer.getInt() == embeddingId) {
            int offset = positionsBuffer.getInt();
            int blockNumber = positionsBuffer.getInt();
            return EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + offset);
        }
        return -1;
    }

    private int findEmbeddingInd(Integer embeddingId, long position) {
        int left = 0;
        int right = (int) position - 1;
        int id;
        while (right - left > 1) {
            positionsBuffer.position((left + right) / 2 * 12);
            id = positionsBuffer.getInt();
            if (id > embeddingId) {
                right = (left + right) / 2;
                continue;
            }
            if (id < embeddingId) {
                left = (left + right) / 2;
                continue;
            }
            return (left + right) / 2 * 12;
        }
        positionsBuffer.position((left + right) / 2 * 12);
        if (positionsBuffer.getInt() == embeddingId) {
            return (left + right) / 2 * 12;
        }
        positionsBuffer.position(right * 12);
        if (positionsBuffer.getInt() == embeddingId) {
            return right * 12;
        }
        return -1;
    }

    private Embedding getEmbeddingByPosition(long embeddingPosition, int embeddingId) throws IOException {
        MemoryMapper mapper = new MemoryMapper();
        mapper.initMapperForReading(pathToEmbeddingsFile, embeddingPosition, EMBEDDING_SIZE_IN_BYTES);
        Embedding embedding = parseEmbedding(mapper.getMappedByteBuffer());
        if (embedding == null || embedding.getId() != embeddingId) {
            throw new StorageOutOfSyncException();
        }
        return embedding;
    }

    private Embedding parseEmbedding(MappedByteBuffer buffer) throws IOException {
        DataReader<Embedding> reader = new EmbeddingReader(buffer);
        if (reader.hasNext()) {
            Embedding embedding = new Embedding(null, EMBEDDING_SIZE, 0);
            reader.readData(embedding);
            return embedding;
        }
        return null;
    }

    public void initMap(int amount) {
        try (RandomAccessFile file = new RandomAccessFile(pathToPositionsFile, "rw");
             FileChannel fileChannel = file.getChannel()) {
            long position = positionsBuffer == null ? 0 : positionsBuffer.limit();
            positionsBuffer = fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, position + amount * 12L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
