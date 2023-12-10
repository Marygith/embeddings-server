package ru.nms.embeddingsserver.jmh;


import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.openjdk.jmh.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.event.RecordApplicationEvents;
import ru.nms.embeddingsserver.model.Embedding;
import ru.nms.embeddingsserver.service.EmbeddingService;
import ru.nms.embeddingsserver.service.SQLiteEmbeddingService;
import ru.nms.embeddingsserver.util.Constants;
import ru.nms.embeddingsserver.util.EmbeddingGenerator;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static ru.nms.embeddingsserver.util.Constants.*;

@Fork(2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 1, time = 200, timeUnit = TimeUnit.MILLISECONDS)
public class EmbeddingsServerBenchmark {

//    private Connection connection;


    private static final EmbeddingService embeddingService = new EmbeddingService();

    private static final SQLiteEmbeddingService sqLiteEmbeddingService = new SQLiteEmbeddingService();

    private static final EmbeddingGenerator generator = new EmbeddingGenerator();

    private static StringBuilder insertQueryFoSQLite;

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @State(Scope.Thread)
    @Getter
    public static class BenchmarkState {

        private List<Embedding> embeddingsForSQLite = new ArrayList<>();

        private List<List<Embedding>> customEmbeddings = new ArrayList<>();

        private Embedding newEmbedding;

        @Setup(Level.Invocation)
        public void doSetup(ExecutionPlan plan) throws IOException, SQLException {
            File metaFile = new File(PATH_TO_META_FILE);
            File embeddingsFile = new File(PATH_TO_EMBEDDINGS_FILE);
            embeddingsFile.delete();
            metaFile.delete();
//            connection = null;
//            connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
            List<Embedding> embeddings = generator.generateNEmbeddings(plan.getEmbeddingsAmount(), 0);
            embeddingService.putEmbeddingsToFile(embeddings);
            sqLiteEmbeddingService.setUpDatabase(plan.getEmbeddingsAmount());
            embeddingsForSQLite.clear();
            embeddingsForSQLite.addAll(generator.generateNEmbeddings(plan.getEmbeddingsAmount(), plan.getEmbeddingsAmount()));
            int index = 0;
            while (index < plan.getEmbeddingsAmount()) {
                customEmbeddings.add(new ArrayList<>(embeddingsForSQLite.subList(index, (int) Math.min(index + BLOCK_SIZE, plan.getEmbeddingsAmount()))));
                index += BLOCK_SIZE;
            }
            newEmbedding = generator.generateNEmbeddings(1, plan.getEmbeddingsAmount() + 1).get(0);
        }
    }

/*    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testCustomCreate(BenchmarkState state) throws IOException {
        for(List<Embedding> embeddings: state.customEmbeddings) {
            embeddingService.putEmbeddingsToFile(embeddings);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSQLiteCreate(ExecutionPlan plan, BenchmarkState state) throws IOException {
        sqLiteEmbeddingService.createTest(plan.getEmbeddingsAmount(), state.getEmbeddingsForSQLite());
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSQLiteUpdate(ExecutionPlan plan, BenchmarkState state) throws IOException {
        sqLiteEmbeddingService.updateTest(state.getNewEmbedding(), plan.getEmbeddingsAmount() / 2);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testCustomUpdate(ExecutionPlan plan, BenchmarkState state) throws IOException {
        embeddingService.updateEmbeddingById(plan.getEmbeddingsAmount() / 2, state.getNewEmbedding());
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSQLiteDelete(ExecutionPlan plan, BenchmarkState state) throws IOException {
        sqLiteEmbeddingService.deleteTest(plan.getEmbeddingsAmount() / 2);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testCustomDelete(ExecutionPlan plan, BenchmarkState state) throws IOException {
        embeddingService.deleteEmbeddingById(plan.getEmbeddingsAmount() / 2);
    }*/

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testSQLiteGet(ExecutionPlan plan, BenchmarkState state) throws IOException {
        sqLiteEmbeddingService.getTest(plan.getEmbeddingsAmount() / 2);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void testCustomGet(ExecutionPlan plan, BenchmarkState state) throws IOException {
        embeddingService.findEmbeddingById(plan.getEmbeddingsAmount() / 2);
    }

}
