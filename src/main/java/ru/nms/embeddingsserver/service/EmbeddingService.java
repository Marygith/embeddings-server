package ru.nms.embeddingsserver.service;

import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Service;
import ru.nms.embeddingsserver.decoder.DataReader;
import ru.nms.embeddingsserver.decoder.EmbeddingReader;
import ru.nms.embeddingsserver.encoder.DataWriter;
import ru.nms.embeddingsserver.encoder.EmbeddingWriter;
import ru.nms.embeddingsserver.exception.EmbeddingNotFoundException;
import ru.nms.embeddingsserver.exception.StorageOutOfSyncException;
import ru.nms.embeddingsserver.model.Embedding;
import ru.nms.embeddingsserver.util.Constants;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static ru.nms.embeddingsserver.util.Constants.*;

@Service
public class EmbeddingService {

    private ByteBuffer buffer = ByteBuffer.allocate(12);

    /**
     * Получение эмбеддинга по id
     *
     * @param id - id документа
     * @return - документ(эмбеддинг)
     * @throws IOException
     */
    public Embedding findEmbeddingById(int id) throws IOException {
//        List<String> lines = FileUtils.readLines(new File(PATH_TO_META_FILE), "utf8");
        long position = findEmbeddingPosition(id/*, FileUtils.readLines(positionsFile)*/);
        if (position == -1) throw new EmbeddingNotFoundException();
//        int index = findEmbeddingInd(id, lines);
//        return getEmbedding(index, lines, id);
        return getEmbeddingByPosition(position, id);
    }


    /**
     * Сохранение эмбеддингов
     *
     * @param embeddings - эмбеддинги
     * @throws IOException
     */
    public void putEmbeddingsToFile(List<Embedding> embeddings) throws IOException {
        Map<String, Long> meta = getMeta();
        writeEmbeddingsToBlock(embeddings, meta.getOrDefault(POSITION, 0L));
        writeMeta(embeddings, meta);
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
        File file = getPositionsFile();
        List<String> lines = FileUtils.readLines(file);
//        List<String> lines = FileUtils.readLines(file, "utf8");
        int index = findEmbeddingInd(id/*, lines*/);
        if (index == 1) return false;
//        lines.remove(index);
//        FileUtils.writeLines(file, lines);
        deletePositionsInfo(index);
        return true;
    }

    private void deletePositionsInfo(int position) {
        try (RandomAccessFile file = new RandomAccessFile(PATH_TO_POSITIONS, "rw")) {
            file.seek(position + 12);

            // Создаем буфер для считывания оставшихся байтов
            byte[] buffer = new byte[(int) (file.length() - position - 12)];

            // Считываем оставшиеся байты в буфер
            file.read(buffer);

            // Перемещаем указатель обратно на позицию, с которой нужно начать удаление
            file.seek(position);

            // Записываем оставшиеся байты обратно в файл
            file.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
//        Map<String, Long> meta = getMeta();
//        File metaFile = new File(PATH_TO_META_FILE);
//        List<String> lines = FileUtils.readLines(metaFile, "utf8");
        File positionsFile = getPositionsFile();
        long position = findEmbeddingPosition(id/*, FileUtils.readLines(positionsFile)*/);
//        int index = findEmbeddingInd(id, FileUtils.readLines(positionsFile, "utf8"));
        if (position == 1) return false;
        embedding.setId(id);
//        long embeddingPosition = parseEmbeddingPosition(index, lines);
        writeEmbeddingsToBlock(List.of(embedding), position);
        return true;
    }

    private void writeEmbeddingsToBlock(List<Embedding> embeddings, long position) {
        MemoryMapper mapper = new MemoryMapper();
        mapper.initMapperForWriting(PATH_TO_EMBEDDINGS_FILE, position, embeddings.size() * EMBEDDING_SIZE_IN_BYTES);
        DataWriter<Embedding> writer = new EmbeddingWriter();
        try {
            writer.create(mapper.getMappedByteBuffer());
            for (Embedding e : embeddings) {
                writer.addData(e);
            }
            writer.getMappedByteBuffer().load();
        } catch (IOException e) {

        }
    }

    private Map<String, Long> getMeta() throws IOException {
        Map<String, Long> meta = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(Constants.PATH_TO_META_FILE))) {
            List<String> keys = List.of(POSITION, BLOCK_NUMBER, AMOUNT_OF_EMBEDDINGS_IN_BLOCK);

            for (String key : keys) {
                String line = reader.readLine();
                if (line == null) break;
                String value = line.substring(line.indexOf(key) + key.length() + 1);
                meta.put(key, Long.parseLong(value));

            }
        } catch (FileNotFoundException e) {
            Path textFilePath = Paths.get(PATH_TO_META_FILE);
            if (Files.notExists(textFilePath)) Files.createFile(textFilePath);
            return new HashMap<>();
        }
        return meta;
    }

    private void writeMeta(List<Embedding> embeddings, Map<String, Long> meta) throws IOException {
        writePositions(embeddings, meta);
        File file = new File(PATH_TO_META_FILE);
        List<String> lines = FileUtils.readLines(file, "utf8");
        if (lines.isEmpty()) lines.addAll(List.of("", "", ""));
        long amountOfEmbeddingsInBlock = meta.getOrDefault(AMOUNT_OF_EMBEDDINGS_IN_BLOCK, 0L);
        long blockNumber = meta.getOrDefault(BLOCK_NUMBER, 0L);
     /*
        for (Embedding embedding : embeddings) {
            if (amountOfEmbeddingsInBlock >= BLOCK_SIZE) {
                amountOfEmbeddingsInBlock = 0;
                blockNumber++;
            }
            lines.add(embedding.getId() + " " + OFFSET + " " + amountOfEmbeddingsInBlock++ + " " + BLOCK_NUMBER + " " + blockNumber);
        }*/
        lines.set(0, POSITION + " " + EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + amountOfEmbeddingsInBlock));
        lines.set(1, BLOCK_NUMBER + " " + blockNumber);
        lines.set(2, AMOUNT_OF_EMBEDDINGS_IN_BLOCK + " " + amountOfEmbeddingsInBlock);
        FileUtils.writeLines(file, lines);

    }

/*    private void writePositions(List<Embedding> embeddings, Map<String, Long> meta) throws IOException {
        File file = getPositionsFile();

        List<String> lines = FileUtils.readLines(file);
        long amountOfEmbeddingsInBlock = meta.getOrDefault(AMOUNT_OF_EMBEDDINGS_IN_BLOCK, 0L);
        long blockNumber = meta.getOrDefault(BLOCK_NUMBER, 0L);
        for (Embedding embedding : embeddings) {
            if (amountOfEmbeddingsInBlock >= BLOCK_SIZE) {
                amountOfEmbeddingsInBlock = 0;
                blockNumber++;
            }
            buffer.clear();
            buffer.putInt(embedding.getId());
            buffer.putInt(Math.toIntExact(amountOfEmbeddingsInBlock++));
            buffer.putInt(Math.toIntExact(blockNumber));
//            lines.add(embedding.getId() + " " + OFFSET + " " + amountOfEmbeddingsInBlock++ + " " + BLOCK_NUMBER + " " + blockNumber);
            lines.add(new String(buffer.array(), StandardCharsets.UTF_8));
        }
        FileUtils.writeLines(file, lines);

    }*/

    private void writePositions(List<Embedding> embeddings, Map<String, Long> meta) {
        try (RandomAccessFile file = new RandomAccessFile(PATH_TO_POSITIONS, "rw")) {
            file.seek(file.length());
            long amountOfEmbeddingsInBlock = meta.getOrDefault(AMOUNT_OF_EMBEDDINGS_IN_BLOCK, 0L);
            long blockNumber = meta.getOrDefault(BLOCK_NUMBER, 0L);
            for (Embedding embedding : embeddings) {
                if (amountOfEmbeddingsInBlock >= BLOCK_SIZE) {
                    amountOfEmbeddingsInBlock = 0;
                    blockNumber++;
                }
                file.writeInt(embedding.getId());
                file.writeInt(Math.toIntExact(amountOfEmbeddingsInBlock++));
                file.writeInt(Math.toIntExact(blockNumber));
//            lines.add(embedding.getId() + " " + OFFSET + " " + amountOfEmbeddingsInBlock++ + " " + BLOCK_NUMBER + " " + blockNumber);
            }
            int a = 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*    private Integer findEmbeddingInd(Integer embeddingId, List<String> lines) throws FileNotFoundException {

            int left = 3;
            int right = lines.size() - 1;
            int id;
            while (right - left > 1) {
                id = Integer.parseInt(lines.get((left + right) / 2).substring(0, lines.get((left + right) / 2).indexOf(" ")));
                if (id > embeddingId) {
                    right = (left + right) / 2;
                    continue;
                }
                if (id < embeddingId) {
                    left = (left + right) / 2;
                    continue;
                }
                return (left + right) / 2;
            }
            if (Integer.parseInt(lines.get((left + right) / 2).substring(0, lines.get((left + right) / 2).indexOf(" "))) == embeddingId) {
                return (left + right) / 2;
            }
            if (Integer.parseInt(lines.get(right).substring(0, lines.get(right).indexOf(" "))) == embeddingId) {
                return right;
            }
            return -1;
        }*/
/*
    private long findEmbeddingPosition(Integer embeddingId, List<String> lines) throws FileNotFoundException {
        int left = 0;
        int right = lines.size() - 1;
        int id;
        ByteBuffer buffer;
        while (right - left > 1) {
            buffer = ByteBuffer.wrap(lines.get((left + right) / 2).getBytes());
            id = buffer.getInt();
//            id = Integer.parseInt(lines.get((left + right) / 2).substring(0, lines.get((left + right) / 2).indexOf(" ")));
            if (id > embeddingId) {
                right = (left + right) / 2;
                continue;
            }
            if (id < embeddingId) {
                left = (left + right) / 2;
                continue;
            }
            int offset = buffer.getInt();
            int blockNumber = buffer.getInt();
            return EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + offset);
//            return (left + right) / 2;
        }
        buffer = ByteBuffer.wrap(lines.get((left + right) / 2).getBytes());
        if (buffer.getInt() == embeddingId) {
            int offset = buffer.getInt();
            int blockNumber = buffer.getInt();
            return EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + offset);
        }
        buffer = ByteBuffer.wrap(lines.get(right).getBytes());
        if (buffer.getInt() == embeddingId) {
            int offset = buffer.getInt();
            int blockNumber = buffer.getInt();
            return EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + offset);
        }
        return -1;

    }
*/

    private long findEmbeddingPosition(Integer embeddingId) throws IOException {
        try (
                RandomAccessFile file = new RandomAccessFile(PATH_TO_POSITIONS, "r");) {

            int left = 0;
            int right = (int) file.length() / 12 - 1;
            int id;
            ByteBuffer buffer;
            while (right - left > 1) {
                file.seek((left + right) / 2 * 12L);
                id = file.readInt();
//            id = Integer.parseInt(lines.get((left + right) / 2).substring(0, lines.get((left + right) / 2).indexOf(" ")));
                if (id > embeddingId) {
                    right = (left + right) / 2;
                    continue;
                }
                if (id < embeddingId) {
                    left = (left + right) / 2;
                    continue;
                }
                int offset = file.readInt();
                int blockNumber = file.readInt();
                return EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + offset);
//            return (left + right) / 2;
            }
            file.seek((left + right) / 2 * 12L);
            if (file.readInt() == embeddingId) {
                int offset = file.readInt();
                int blockNumber = file.readInt();
                return EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + offset);
            }
            file.seek(right * 12L);

            if (file.readInt() == embeddingId) {
                int offset = file.readInt();
                int blockNumber = file.readInt();
                return EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + offset);
            }
            return -1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int findEmbeddingInd(Integer embeddingId) {
        try (
                RandomAccessFile file = new RandomAccessFile(PATH_TO_POSITIONS, "r");) {

            int left = 0;
            int right = (int) file.length() / 12;
            int id;
            ByteBuffer buffer;
            while (right - left > 1) {
                file.seek((left + right) / 2 * 12L);
                id = file.readInt();
//            id = Integer.parseInt(lines.get((left + right) / 2).substring(0, lines.get((left + right) / 2).indexOf(" ")));
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
            file.seek((left + right) / 2 * 12L);
            if (file.readInt() == embeddingId) {
                return (left + right) / 2 * 12;
            }
            file.seek(right * 12L);
            if (file.readInt() == embeddingId) {
                return right * 12;
            }
            return -1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /*private int findEmbeddingInd(Integer embeddingId, List<String> lines) {
        int left = 0;
        int right = lines.size() - 1;
        int id;
        ByteBuffer buffer;
        while (right - left > 1) {
            buffer = ByteBuffer.wrap(lines.get((left + right) / 2).getBytes());
            id = buffer.getInt();
//            id = Integer.parseInt(lines.get((left + right) / 2).substring(0, lines.get((left + right) / 2).indexOf(" ")));
            if (id > embeddingId) {
                right = (left + right) / 2;
                continue;
            }
            if (id < embeddingId) {
                left = (left + right) / 2;
                continue;
            }
            return (left + right) / 2;
        }
        buffer = ByteBuffer.wrap(lines.get((left + right) / 2).getBytes());
        if (buffer.getInt() == embeddingId) {
            return (left + right) / 2;

        }
        buffer = ByteBuffer.wrap(lines.get(right).getBytes());
        if (buffer.getInt() == embeddingId) {
            return right;
        }
        return -1;

    }
*/

/*    private long parseEmbeddingPosition(int indexOfEmbeddingInMeta, List<String> embeddingsMeta) {
        if (indexOfEmbeddingInMeta == -1) throw new EmbeddingNotFoundException();
        String line = embeddingsMeta.get(indexOfEmbeddingInMeta);
        int indexOfOffset = line.indexOf(OFFSET);
        int indexBlockNumber = line.indexOf(BLOCK_NUMBER);
        int offset = Integer.parseInt(line.substring(indexOfOffset + OFFSET.length() + 1, line.indexOf(' ', indexOfOffset + OFFSET.length() + 1)));
        int blockNumber = Integer.parseInt(line.substring(indexBlockNumber + BLOCK_NUMBER.length() + 1));
        return EMBEDDING_SIZE_IN_BYTES * (blockNumber * BLOCK_SIZE + offset);
    }*/

/*    private Embedding getEmbedding(int indexOfEmbeddingInMeta, List<String> embeddingsMeta, int embeddingId) throws IOException {
        long embeddingPosition = parseEmbeddingPosition(indexOfEmbeddingInMeta, embeddingsMeta);
        MemoryMapper mapper = new MemoryMapper();
        mapper.initMapperForReading(PATH_TO_EMBEDDINGS_FILE, embeddingPosition, EMBEDDING_SIZE_IN_BYTES);
        Embedding embedding = parseEmbedding(mapper.getMappedByteBuffer());
        if (embedding == null || embedding.getId() != embeddingId) {
            throw new StorageOutOfSyncException();
        }
        return embedding;
    }*/

    private Embedding getEmbeddingByPosition(long embeddingPosition, int embeddingId) throws IOException {
//        long embeddingPosition = parseEmbeddingPosition(indexOfEmbeddingInMeta, embeddingsMeta);
        MemoryMapper mapper = new MemoryMapper();
        mapper.initMapperForReading(PATH_TO_EMBEDDINGS_FILE, embeddingPosition, EMBEDDING_SIZE_IN_BYTES);
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

    private File getPositionsFile() throws IOException {
        Path textFilePath = Paths.get(PATH_TO_POSITIONS);
        if (Files.notExists(textFilePath)) Files.createFile(textFilePath);
        return new File(PATH_TO_POSITIONS);
    }
}
