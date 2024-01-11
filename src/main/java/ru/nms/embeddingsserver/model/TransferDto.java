package ru.nms.embeddingsserver.model;

import lombok.Builder;


@Builder
public record TransferDto(int hash, int port, String address) {

}
