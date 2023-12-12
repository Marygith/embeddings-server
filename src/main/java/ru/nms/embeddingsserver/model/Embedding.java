package ru.nms.embeddingsserver.model;

import lombok.*;

import java.util.Arrays;

@Getter
@ToString
@EqualsAndHashCode
public class Embedding {

    @Setter
    private int id;

    private final int embeddingSize;

    @Setter
    @ToString.Exclude
    private float[][] embedding;

    public Embedding(float[][] embedding, int embeddingSize, int id) {
        this.embedding = embedding;
        this.embeddingSize = embeddingSize;
        this.id = id;
    }
}
