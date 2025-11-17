package com.davdb.davdb.infra.db.impl;

import com.davdb.davdb.infra.db.DavDB;
import com.davdb.davdb.infra.persistance.Memtable;
import com.davdb.davdb.infra.persistance.SSTableReader;
import com.davdb.davdb.infra.persistance.serialization.Serializer;

import java.util.concurrent.ExecutionException;


public class DavDBImpl<K,V> implements DavDB<K, V> {

    private Memtable<K, V> memtable;
    private SSTableReader<K, V> chunkReader;


    public DavDBImpl(Serializer<K> keySerializer, Serializer<V> urlInfoSerializer) {
        this.memtable = new Memtable<>(keySerializer, urlInfoSerializer);
        this.chunkReader = new SSTableReader<>(keySerializer, urlInfoSerializer);
    }

    @Override
    public V put(K key, V value) {

        try {
            return memtable.insert(key, value);
        }catch (InterruptedException | ExecutionException e) {
            System.out.println("[Davdb] An error was happen when trying to wait record being saving on WAL ");
        }

        return null;
    }

    @Override
    public V delete(K key) {
        return null;
    }

    @Override
    public V search(K key) {
        return null;
    }
}
