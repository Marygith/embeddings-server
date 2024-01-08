package ru.nms.embeddingsserver.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class HashNotFoundException extends ResponseStatusException {
    public HashNotFoundException() {
        super(HttpStatus.NOT_FOUND, "Embeddings with given hash were not found");
    }
}
