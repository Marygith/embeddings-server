package ru.nms.embeddingsserver.model;

import lombok.Builder;

@Builder
public record TransferRequest(int hash, String address, int port) {
}
