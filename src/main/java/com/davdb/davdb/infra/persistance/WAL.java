package com.davdb.davdb.infra.persistance;

import com.davdb.davdb.infra.persistance.entity.WALLine;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;


public class WAL<K, V> {

    private final BufferedWriter logWriter;
    private final AtomicInteger lsnCounter = new AtomicInteger(0);
    private final BlockingQueue<WALLine> queue = new ArrayBlockingQueue<>(1_000);


    public WAL() throws IOException {
        String FILE_LOG_PATH = "data/davdb.wal";
        FileWriter fileWriter = new FileWriter(FILE_LOG_PATH, true);
        logWriter = new BufferedWriter(fileWriter);
        startWritePooling();
    }

    public CompletableFuture<Boolean> write(K key, V value)  {
        String WAL_SEPARATOR = "|";
        String checksumValue = key + WAL_SEPARATOR + value.toString();
        CRC32 crc32 = new CRC32();
        crc32.update(checksumValue.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder()
                .append(lsnCounter.incrementAndGet())
                        .append(WAL_SEPARATOR)
                                .append(key)
                                        .append(WAL_SEPARATOR)
                                                .append(value)
                                                        .append(WAL_SEPARATOR)
                                                                .append(crc32.getValue());


        CompletableFuture<Boolean> ack = new CompletableFuture<>();
        boolean hasSaved = this.queue.offer(new WALLine(sb.toString(),ack));

        if (!hasSaved) {
            throw new RuntimeException("[WAL] error on sending write command to WAL queue");
        }
        return ack;
    }

    private void startWritePooling() {
        Thread.ofVirtual().start(() -> {
            System.out.println("[WAL] Stating background thread to save records in WAL");
            while (true) {
                try {
                    WALLine walLine = queue.take();
                    logWriter.write(walLine.getLine());
                    logWriter.newLine();
                    logWriter.flush();
                    walLine.getAck().complete(true);
                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
