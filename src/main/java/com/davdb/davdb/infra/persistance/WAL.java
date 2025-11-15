package com.davdb.davdb.infra.persistance;

import com.davdb.davdb.infra.persistance.entity.Entry;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;


public class WAL<K, V> {

    private final BufferedWriter logWriter;
    private final AtomicInteger lsnCounter = new AtomicInteger(0);
    private final String WAL_SEPARATOR = "|";
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(1_000);


    public WAL() throws IOException {
        String FILE_LOG_PATH = "data/log.davdbwal";
        FileWriter fileWriter = new FileWriter(FILE_LOG_PATH, true);
        logWriter = new BufferedWriter(fileWriter);
        startWritePooling();
    }

    public void write(Entry<K,V> entry)  {
        String checksumValue = entry.getkey().toString() + WAL_SEPARATOR + entry.getValue().toString();
        CRC32 crc32 = new CRC32();
        crc32.update(checksumValue.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder()
                .append(lsnCounter.incrementAndGet())
                        .append(WAL_SEPARATOR)
                                .append(entry.getkey().toString())
                                        .append(WAL_SEPARATOR)
                                                .append(entry.getValue().toString())
                                                        .append(WAL_SEPARATOR)
                                                                .append(crc32.getValue());

        boolean hasSaved = this.queue.offer(sb.toString());

        if (!hasSaved) {
            throw new RuntimeException("[WAL] error on sending write command to WAL queue");
        }
    }

    private void startWritePooling() {
        Thread.ofVirtual().start(() -> {
            System.out.println("[WAL] Stating background thread to save records in WAL");
            while (true) {
                try {
                    String line = queue.take();
                    logWriter.write(line);
                    logWriter.newLine();
                    logWriter.flush();
                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
