package com.davdb.davdb.infra.persistance;

import com.davdb.davdb.infra.persistance.entity.Entry;
import com.davdb.davdb.infra.persistance.serialization.Serializer;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

public class Memtable<K, V> {

    private TreeMap<K, V> table = new TreeMap<>();
    private final Integer MEMTABLE_SIZE_LIMIT = 10;
    private boolean frozen = false;

    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;


    public Memtable(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }



    public V insert(Entry<K, V> entry) throws Exception {

        if(this.frozen) throw new Exception("Attempting to write to memtable while it`s frozen");

        V result = table.put(entry.getkey(), entry.getValue());

        if(table.size() >= MEMTABLE_SIZE_LIMIT) {
            rotate();
        }

        return result;
    }

    public void printdata() {
        System.out.println("[MEMTABLE] start printing data...");

        int line = 1;
        for(K key : this.table.keySet()) {
            System.out.println("[MEMTABLE] {"+line+"} "+key+": "+ this.table.get(key));
            line++;
        }

        System.out.println("[MEMTABLE] end printing data...");
    }

    public void rotate() {
        freezeToggler();
        SortedMap<K,V> memtableToFlush = Collections.unmodifiableSortedMap(table);
        flush(memtableToFlush);
        table = new TreeMap<>();
        freezeToggler();
    }

    private void freezeToggler(){
        this.frozen = !this.frozen;
    }

    private void flush(SortedMap<K,V> table) {

        Runnable writeToDisk = new Runnable() {
            @Override
            public void run() {
                new SStable<>(table, keySerializer, valueSerializer).writeToFile();
            }
        };

       Thread.ofVirtual().start(writeToDisk);
    }
}