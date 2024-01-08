package ru.nms.embeddingsserver.model;

import lombok.Builder;

import java.util.List;


@Builder
public record TransferDto(int hash, int port, String address) {

}
