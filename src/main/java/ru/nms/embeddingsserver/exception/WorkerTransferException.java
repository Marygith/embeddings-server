package ru.nms.embeddingsserver.exception;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class WorkerTransferException extends RuntimeException {

    private final String destinationUrl;

    @Override
    public String getMessage() {
        return "transfer to " + destinationUrl + " failed";
    }
}
