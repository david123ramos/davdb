package com.davdb.davdb.infra.persistance;

import com.davdb.davdb.infra.persistance.entity.Entry;
import com.davdb.davdb.infra.persistance.serialization.Serializer;

import java.util.Collections;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Memtable<K, V> {

    private AtomicReference<ConcurrentSkipListMap<K, V>> table = new AtomicReference<>(new ConcurrentSkipListMap<>());
    private final Integer MEMTABLE_SIZE_LIMIT = 1_000;

    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;
    AtomicBoolean rotating = new AtomicBoolean(false);
    AtomicInteger sz = new AtomicInteger(0);


    public Memtable(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public V insert(Entry<K, V> entry) throws Exception {

        SortedMap<K,V> currentTable = table.get();
        V result = currentTable.put(entry.getkey(), entry.getValue());

        if(result == null && sz.incrementAndGet() >= MEMTABLE_SIZE_LIMIT && rotating.compareAndSet(false, true)) {
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

        Runnable writeToDisk = new Runnable() {
            @Override
            public void run() {
                new SStable<>(table, keySerializer, valueSerializer).writeToFile();
                rotating.set(false);
            }
        };

       Thread.ofVirtual().start(writeToDisk);
    }
}