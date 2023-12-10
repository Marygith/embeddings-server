package ru.nms.embeddingsserver;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.nms.embeddingsserver.model.Embedding;
import ru.nms.embeddingsserver.util.Constants;
import ru.nms.embeddingsserver.util.EmbeddingGenerator;

import static org.junit.jupiter.api.Assertions.*;


import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootTest
public class SQLiteEmbeddingsTests {

    private static Connection connection;

    @Autowired
    private EmbeddingGenerator generator;

    @Test
    void createAndGetTest() {
//        Statement statement = connection.createStatement();
        List<Embedding> embeddings = generator.generateNEmbeddings(5, 5);
        try (PreparedStatement preparedStatement = connection.prepareStatement("insert into embeddings values(?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)");
             Statement statement = connection.createStatement();
        ) {
            int index = 1;
            while (index < 15) {

                preparedStatement.setInt(index, embeddings.get((index - 1) / 3).getId());
                index++;
                preparedStatement.setInt(index, embeddings.get((index - 1) / 3).getEmbeddingSize());
                index++;
                preparedStatement.setBytes(index, vectorsToBytes(embeddings.get((index - 1) / 3).getEmbedding(), Constants.EMBEDDING_SIZE));
                index++;
            }

            preparedStatement.execute();
            ResultSet rs = statement.executeQuery("select * from embeddings where id > 5");
            index = 0;
            while (rs.next()) {
                assertEquals(embeddings.get(index).getId(), rs.getInt("id"));
                assertTrue(Arrays.deepEquals(embeddings.get(index).getEmbedding(), bytesToVectors(Constants.EMBEDDING_SIZE, rs.getBytes("vectors"))));
                assertEquals(embeddings.get(index++).getEmbeddingSize(), rs.getInt("size"));

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void updateTest() {
        //update embedding with id = 2
        Embedding newEmbedding = generator.generateNEmbeddings(1, 1).get(0);
        String updateSQL = "UPDATE embeddings "
                + "SET size = ?,  vectors = ?"
                + "WHERE id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL);
             Statement statement = connection.createStatement();
        ) {


            pstmt.setInt(1, newEmbedding.getEmbeddingSize());
            pstmt.setBytes(2, vectorsToBytes(newEmbedding.getEmbedding(), newEmbedding.getEmbeddingSize()));
            pstmt.setInt(3, newEmbedding.getId());

            pstmt.executeUpdate();
            //validating updated embedding
            ResultSet rs = statement.executeQuery("SELECT * FROM embeddings WHERE id = 2");
            while (rs.next()) {
                assertEquals(newEmbedding.getId(), rs.getInt("id"));
                assertTrue(Arrays.deepEquals(newEmbedding.getEmbedding(), bytesToVectors(Constants.EMBEDDING_SIZE, rs.getBytes("vectors"))));
                assertEquals(newEmbedding.getEmbeddingSize(), rs.getInt("size"));

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void deleteTest() {
        try (PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM embeddings " +
                "WHERE id = 2");
             Statement statement = connection.createStatement();
        ) {
            preparedStatement.executeUpdate();
            ResultSet rs = statement.executeQuery("SELECT * FROM embeddings WHERE id = 2");
            assertFalse(rs.next());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void setUpDatabase() throws SQLException {
        connection = null;
        // create a database connection
        connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
        try (Statement statement = connection.createStatement();
             PreparedStatement preparedStatement = connection.prepareStatement("insert into embeddings values(?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)");

        ) {

            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            statement.executeUpdate("drop table if exists embeddings");
            statement.executeUpdate("create table embeddings (id integer, size integer, vectors blob)");
//        Statement statement = connection.createStatement();
            List<Embedding> embeddings = generator.generateNEmbeddings(5, 0);
            int index = 1;
            while (index < 15) {

                preparedStatement.setInt(index, embeddings.get((index - 1) / 3).getId());
                index++;
                preparedStatement.setInt(index, embeddings.get((index - 1) / 3).getEmbeddingSize());
                index++;
                preparedStatement.setBytes(index, vectorsToBytes(embeddings.get((index - 1) / 3).getEmbedding(), Constants.EMBEDDING_SIZE));
                index++;
            }

            preparedStatement.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] vectorsToBytes(float[][] vectors, int vectorSize) throws IOException, SQLException {

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        int amount = vectors.length;
        dout.writeInt(amount);
        for (int i = 0; i < amount; i++) {
            for (int k = 0; k < vectorSize; k++) {
                dout.writeShort(Float.floatToFloat16(vectors[i][k]));
            }
        }
        dout.close();

        return bout.toByteArray();
    }

    public float[][] bytesToVectors(int vectorSize, byte[] asBytes) throws IOException, SQLException {
        ByteArrayInputStream bin = new ByteArrayInputStream(asBytes);
        DataInputStream din = new DataInputStream(bin);

        int amount = din.readInt();
        float[][] vectors = new float[amount][vectorSize];
        for (int i = 0; i < amount; i++) {
            for (int k = 0; k < vectorSize; k++) {
                vectors[i][k] = Float.float16ToFloat(din.readShort());
            }

        }
        return vectors;
    }
}
