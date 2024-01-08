package ru.nms.embeddingsserver.model;

import lombok.Builder;

@Builder
public record EmbeddingDto(int hash, int id) {
}
