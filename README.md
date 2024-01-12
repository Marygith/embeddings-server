# Worker service for embeddings retrieval project

## Overview

Worker service, that carries out all business logic(getting, storing and retrieving embeddings by id). 

## Storage

Embeddings are encoded in bytes and read and written via mmap for fast retrieval. (more on encoding and decoding in [embeddings-avro ](https://github.com/Marygith/embeddings-avro).

## Receiving

Data wth new embeddings can arrive anytime and is pretty heavy, that's why ServerSockets are used for this purposed, which are opened at the start of worker instance and ready to accept concurrent connections.

## Delivery

To speed up embeddings retrieval, MappedByteBuffer opens when new embeddings are received and stays open - this way costs for opening\closing file channel are avoided.

## Requirements

This project requires java 21 and running zookeeper(by defult address - localhost:2181)
