package ru.nms.embeddingsserver.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import ru.nms.embeddingslibrary.model.Embedding;
import ru.nms.embeddingslibrary.model.TransferRequest;
import ru.nms.embeddingsserver.exception.EmbeddingNotFoundException;
import ru.nms.embeddingsserver.exception.HashNotFoundException;
import ru.nms.embeddingsserver.exception.WorkerTransferException;
import ru.nms.embeddingsserver.service.RetrievalService;
import ru.nms.embeddingsserver.service.TransferService;

@RestController
@RequestMapping("/worker")
@RequiredArgsConstructor
@Slf4j
public class WorkerServiceController {

    private final TransferService transferService;

    private final RetrievalService retrievalService;

    @PostMapping("/transfer")
    public void transferEmbeddings(@RequestBody TransferRequest transferRequest) {
        //todo log.info("came request to transfer embeddings with hash " + transferRequest.hash() + " to " + transferRequest.address() + ":" + transferRequest.port());
        byte[] embeddings = retrievalService.getEmbeddingsAsBytesByHash(transferRequest.hash());
        transferService.transferEmbeddingsTo(transferRequest.hash(), transferRequest.port() - 10, embeddings);
//        retrievalService.deleteEmbeddingsByHash(transferRequest.hash()); //todo implement real deleting of files

    }

    @GetMapping("/embeddings/{hash}/{id}")
    public Embedding getEmbedding(@PathVariable int hash, @PathVariable int id) {
        log.info("came request to retrieve embedding with id " + id + " and hash " + hash);
        return retrievalService.getEmbeddingById(hash, id);
    }

    @ExceptionHandler({
            EmbeddingNotFoundException.class,
            HashNotFoundException.class,
            WorkerTransferException.class
    })
    public ResponseEntity<String> handleBadRequestExceptions(
            ResponseStatusException exception
    ) {
        return ResponseEntity
                .status(exception.getStatus())
                .body(exception.getMessage());
    }
}
