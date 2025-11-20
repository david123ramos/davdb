package com.davdb.davdb.infra.persistance;

import com.davdb.davdb.infra.persistance.serialization.Serializer;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Memtable<K extends Comparable<K>, V> {

    private AtomicReference<ConcurrentSkipListMap<K, V>> table = new AtomicReference<>(new ConcurrentSkipListMap<>());
    private final Integer MEMTABLE_SIZE_LIMIT = 1_000;
    private final WAL<K,V> WALService;

    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;
    AtomicBoolean rotating = new AtomicBoolean(false);
    AtomicInteger sz = new AtomicInteger(0);

    boolean isSynchronousCommitActive = Boolean.parseBoolean(System.getenv("DAVDB_SYNCHRONOUS_WAL_COMMIT_ACTIVE"));

    public Memtable(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        System.out.println("[MEMTABLE - WAL persistance type] synchronous commit is " + (this.isSynchronousCommitActive ? "active" : "deactivated"));

        try {
            this.WALService = new WAL<>();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("[MEMTABLE] Was not possible to create log file");
        }

    }

    public V insert(K key, V value) throws ExecutionException, InterruptedException {

        CompletableFuture<Boolean> resultWalLine = WALService.write(key, value);

        if(isSynchronousCommitActive) resultWalLine.get();

        SortedMap<K,V> currentTable = table.get();
        V result = currentTable.put(key, value);

        if(result != null) return  result;

        //it's not completely guaranteed that flushed sstable will contain exactly MEMTABLE_SIZE_LIMIT entries.
        //Because incrementAndGet and check of rotating flag can happen concurrently. There`s a race condition.
        //It means that flushed file can contain more entries than the limit defined even if the thread that has inserted
        //into memtable wasn`t the one  responsible for calling the rotating function.
        if(sz.incrementAndGet() >= MEMTABLE_SIZE_LIMIT && rotating.compareAndSet(false, true)) {
            System.out.println("[MEMTABLE] Limit reached! Flushing data");
            try{
                rotate();
            }finally {
                sz.set(0);
            }
        }

        return result;
    }

    public void rotate() {
        SortedMap<K,V> memtableToFlush = table.getAndSet(new ConcurrentSkipListMap<>());
        flush(Collections.unmodifiableSortedMap(memtableToFlush));
    }

    private void flush(SortedMap<K,V> table) {

        Runnable writeToDisk = () -> {
            new SStable<>(table, keySerializer, valueSerializer).writeToFile();
            rotating.set(false);
        };

       Thread.ofVirtual().start(writeToDisk);
    }
}