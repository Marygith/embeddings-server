package ru.nms.embeddingsserver.service;

import ru.nms.embeddingsserver.model.Embedding;
import ru.nms.embeddingsserver.util.Constants;

import java.io.*;
import java.sql.*;
import java.util.List;

public class SQLiteEmbeddingService {

    private static Connection connection;

    private String insertQuery;

    public SQLiteEmbeddingService() {
    }

    public Embedding getTest(int id) {
        try (Statement statement = connection.createStatement();) {
            ResultSet rs = statement.executeQuery("SELECT * FROM embeddings WHERE id = " + id);

            if (rs.next()) {
                return new Embedding(bytesToVectors(Constants.EMBEDDING_SIZE, rs.getBytes("vectors")), rs.getInt("size"), rs.getInt("id"));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException();
    }

    public void createTest(int embeddingsAmount, List<Embedding> embeddings) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
            int index = 1;
            while (index < embeddingsAmount * 3) {

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


    public void updateTest(Embedding newEmbedding, int id) {
        String updateSQL = "UPDATE embeddings "
                + "SET size = ?,  vectors = ?"
                + "WHERE id = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
            pstmt.setInt(1, newEmbedding.getEmbeddingSize());
            pstmt.setBytes(2, vectorsToBytes(newEmbedding.getEmbedding(), newEmbedding.getEmbeddingSize()));
            pstmt.setInt(3, id);

            pstmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void deleteTest(int id) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM embeddings " +
                "WHERE id = ?")) {
            preparedStatement.setInt(1, id);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setUpDatabase(int embeddingsAmount, List<Embedding> embeddings) throws SQLException {
        connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
        insertQuery = "insert into embeddings values(?, ?, ?)" + ", (?, ?, ?)"
                .repeat(Math.max(0, embeddingsAmount - 1));
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            statement.executeUpdate("drop table if exists embeddings");
            statement.executeUpdate("create table embeddings (id integer, size integer, vectors blob)");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
            int index = 1;
            while (index < embeddingsAmount * 3) {

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
        for (float[] vector : vectors) {
            for (int k = 0; k < vectorSize; k++) {
                dout.writeShort(Float.floatToFloat16(vector[k]));
            }
        }
        dout.close();

        return bout.toByteArray();
    }

    private float[][] bytesToVectors(int vectorSize, byte[] asBytes) throws IOException, SQLException {
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
