package ru.nms.embeddingsserver.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import ru.nms.embeddingsserver.exception.WorkerTransferException;
import ru.nms.embeddingsserver.model.TransferDto;

import javax.annotation.PostConstruct;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransferService {

    @Value("${server.port}")
    private int port;
    private final RestTemplate restTemplate = new RestTemplate();

    private final RetrievalService retrievalService;
    private final HttpHeaders headers = new HttpHeaders();

    private DataOutputStream dataOutputStream = null;
    private DataInputStream dataInputStream = null;

    private ServerSocket serverSocket;

    private String url;

    @PostConstruct
    private void init() {
        headers.setContentType(MediaType.APPLICATION_JSON);
        final ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);

        Runnable acceptConnectionsTask = () -> {
            try {
                serverSocket = new ServerSocket(port - 10);
                log.info("Waiting for clients to connect...");
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    log.info("Accepted connection");
                    clientProcessingPool.submit(new ReceiveEmbeddingsTask(clientSocket));
                }
            } catch (IOException e) {
                log.error("Unable to process client request");
            }
        };
        Thread serverThread = new Thread(acceptConnectionsTask);
        serverThread.start();

        log.info("Finished transfer service initialization");
    }

    public void transferEmbeddingsTo(int embeddingsHash, int port, byte[] embeddings) {

        try (
                Socket socket = new Socket("localhost", port);
        ) {

            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(embeddingsHash);
            dataOutputStream.writeLong(embeddings.length);

            int chunkSize = 1024; // Chunk size in bytes
            int offset = 0;
            while (offset < embeddings.length) {
                int length = Math.min(chunkSize, embeddings.length - offset);
                byte[] chunk = Arrays.copyOfRange(embeddings, offset, offset + length);
                // Write the chunk to the DataOutputStream
                dataOutputStream.write(chunk);
                offset += length;
            }

            dataOutputStream.flush();
            dataOutputStream.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private class ReceiveEmbeddingsTask implements Runnable {
        private final Socket clientSocket;

        private ReceiveEmbeddingsTask(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {

            try {


                dataInputStream = new DataInputStream(clientSocket.getInputStream());
                int hash = dataInputStream.readInt();

                long embeddingsSize = dataInputStream.readLong();

                byte[] embeddings = dataInputStream.readAllBytes();

                ByteBuffer buffer = ByteBuffer.wrap(embeddings);

                dataInputStream.close();
                clientSocket.close();
                retrievalService.putEmbeddingsByHashFromByteBuffer(hash, buffer);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                clientSocket.close();
            } catch (IOException e) {
                log.error(e.getLocalizedMessage());
            }
        }
    }

}
