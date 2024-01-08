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

    //    private Socket clientSocket;
    private String url;

    @PostConstruct
    private void init() {
        headers.setContentType(MediaType.APPLICATION_JSON);
        final ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);

        Runnable acceptConnectionsTask = () -> {
            try {
                serverSocket = new ServerSocket(port - 10);
                System.out.println("Waiting for clients to connect...");
                while (true) {
                    Socket clientSocket = serverSocket.accept();
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

    public void transferEmbeddingsTo(int embeddingsHash, String address, int port, byte[] embeddings) {
        HttpEntity<TransferDto> requestEntity = new HttpEntity<>(TransferDto.builder().hash(embeddingsHash).address("localhost").port(port).build(), headers);

        try {
            url = createUrl(address, port, "/receive");
            restTemplate.postForObject(url, requestEntity, ResponseEntity.class);

        } catch (RestClientException e) {
            throw new WorkerTransferException(url);
        }
        try (
                Socket socket = new Socket("localhost", port);
        ) {

            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeLong(embeddings.length);
            log.info("Sending " + embeddings.length + " embeddings");

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

    public static String createUrl(String address, int port, String endPoint) {
        return "http://" + address + ":" + port + endPoint;
    }

    private class ReceiveEmbeddingsTask implements Runnable {
        private final Socket clientSocket;

        private ReceiveEmbeddingsTask(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {

            try {

                log.info("Client connection accepted");

                dataInputStream = new DataInputStream(clientSocket.getInputStream());
                int hash = dataInputStream.readInt();
                log.info("Hash equals " + hash);
                long embeddingsSize = dataInputStream.readLong();
                log.info("Receiving " + embeddingsSize + " bytes");

                byte[] embeddings = dataInputStream.readAllBytes();
                log.info("All bytes are written");
                log.info("got byte array with size " + embeddings.length);
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
