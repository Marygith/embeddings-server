package ru.nms.embeddingsserver.jmh;


import lombok.Getter;
import org.openjdk.jmh.annotations.*;
import ru.nms.embeddingsserver.service.EmbeddingService;
import ru.nms.embeddingsserver.service.SQLiteEmbeddingService;
import ru.nms.embeddingsserver.util.EmbeddingGenerator;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static ru.nms.embeddingsserver.util.Constants.BLOCK_SIZE;
import static ru.nms.embeddingsserver.util.Constants.PATH_TO_EMBEDDINGS_DIRECTORY;

import ru.nms.embeddingslibrary.model.Embedding;

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 8, time = 200, timeUnit = TimeUnit.MILLISECONDS)
public class EmbeddingsServerBenchmark {

    private static final EmbeddingService embeddingService = new EmbeddingService();

    private static final SQLiteEmbeddingService sqLiteEmbeddingService = new SQLiteEmbeddingService();

    private static final EmbeddingGenerator generator = new EmbeddingGenerator();


    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @State(Scope.Thread)
    @Getter
    public static class BenchmarkState {

        private final List<Embedding> embeddingsForSQLite = new ArrayList<>();

        private final List<List<Embedding>> customEmbeddings = new ArrayList<>();

        private Embedding newEmbedding;

        @Setup(Level.Invocation)
        public void doSetup(ExecutionPlan plan) throws IOException, SQLException {
            long dirname = System.currentTimeMillis();
            new File(PATH_TO_EMBEDDINGS_DIRECTORY + dirname).mkdirs();
            embeddingService.setPathToEmbeddingsFile(PATH_TO_EMBEDDINGS_DIRECTORY + dirname + "\\embeddings.hasd");
            embeddingService.setPathToMetaFile(PATH_TO_EMBEDDINGS_DIRECTORY + dirname + "\\meta.txt");
            embeddingService.setPathToPositionsFile(PATH_TO_EMBEDDINGS_DIRECTORY + dirname + "\\positions.hasd");

            List<Embedding> embeddings = generator.generateNEmbeddings(plan.getEmbeddingsAmount(), 0);
            embeddingsForSQLite.clear();
            embeddingsForSQLite.addAll(generator.generateNEmbeddings(plan.getEmbeddingsAmount(), plan.getEmbeddingsAmount()));

            int index = 0;
            while (index < plan.getEmbeddingsAmount()) {
                customEmbeddings.add(new ArrayList<>(embeddingsForSQLite.subList(index, (int) Math.min(index + BLOCK_SIZE, plan.getEmbeddingsAmount()))));
                index += BLOCK_SIZE;
            }

            embeddingService.putEmbeddingsToFile(embeddings);
            sqLiteEmbeddingService.setUpDatabase(plan.getEmbeddingsAmount(), embeddings);

            newEmbedding = generator.generateNEmbeddings(1, plan.getEmbeddingsAmount() + 1).get(0);
            System.out.println("directory name " + dirname);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testCustomCreate(BenchmarkState state) throws IOException {
        for (List<Embedding> embeddings : state.customEmbeddings) {
            embeddingService.putEmbeddingsToFile(embeddings);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSQLiteCreate(ExecutionPlan plan, BenchmarkState state) {
        sqLiteEmbeddingService.createTest(plan.getEmbeddingsAmount(), state.getEmbeddingsForSQLite());
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSQLiteUpdate(ExecutionPlan plan, BenchmarkState state) {
        sqLiteEmbeddingService.updateTest(state.getNewEmbedding(), plan.getEmbeddingsAmount() / 2);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testCustomUpdate(ExecutionPlan plan, BenchmarkState state) throws IOException {
        embeddingService.updateEmbeddingById(plan.getEmbeddingsAmount() / 2, state.getNewEmbedding());
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSQLiteDelete(ExecutionPlan plan, BenchmarkState state) {
        sqLiteEmbeddingService.deleteTest(plan.getEmbeddingsAmount() / 2);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testCustomDelete(ExecutionPlan plan, BenchmarkState state) throws IOException {
        embeddingService.deleteEmbeddingById(plan.getEmbeddingsAmount() / 2);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSQLiteGet(ExecutionPlan plan, BenchmarkState state) {
        sqLiteEmbeddingService.getTest(plan.getEmbeddingsAmount() / 2);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testCustomGet(ExecutionPlan plan, BenchmarkState state) throws IOException {
        embeddingService.findEmbeddingById(plan.getEmbeddingsAmount() / 2);
    }

}
