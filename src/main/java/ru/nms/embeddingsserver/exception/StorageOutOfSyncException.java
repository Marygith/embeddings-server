package ru.nms.embeddingsserver.exception;


public class StorageOutOfSyncException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Storage needs to be synced with metadata";
    }
}
