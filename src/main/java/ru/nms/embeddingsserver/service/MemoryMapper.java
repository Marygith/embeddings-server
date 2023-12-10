package ru.nms.embeddingsserver.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.Cleaner;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import java.security.PrivilegedAction;


@Getter
@Component
@Slf4j
public class MemoryMapper {
    private MappedByteBuffer mappedByteBuffer;

    public void initMapperForReading(String fileName, long position, long  length) {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
             FileChannel fileChannel = file.getChannel();)
        {
            mappedByteBuffer = fileChannel
                    .map(FileChannel.MapMode.READ_ONLY, position,length);
            int a = 0;
        } catch (Exception e) {
            log.atError().log("initialization failed due to " + e.getMessage());
        }
    }

    public void initMapperForWriting(String fileName, long position, long length) {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "rw");
             FileChannel fileChannel = file.getChannel();)
        {
            mappedByteBuffer = fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, position, length);
        } catch (Exception e) {
            log.atError().log("initialization failed due to " + e.getMessage());
        }
    }

    public void close() {
        mappedByteBuffer = null;
    }

}
