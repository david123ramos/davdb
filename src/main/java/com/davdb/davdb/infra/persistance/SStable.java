package com.davdb.davdb.infra.persistance;

import com.davdb.davdb.infra.persistance.serialization.Serializer;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.SortedMap;

public class SStable<K,V> {
    private final SortedMap<K,V> map;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public SStable(SortedMap<K, V> map, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.map = map;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public void writeToFile(){
        File file = new File("sstable_"+ LocalDateTime.now().toInstant(ZoneOffset.UTC)+".davtable");

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(file)))) {

            for (Map.Entry<K, V> entry : this.map.entrySet()) {

                keySerializer.write(entry.getKey(), dos);
                valueSerializer.write(entry.getValue(), dos);
            }

            dos.flush();
            System.out.println("[SSTable] Flushed binary SSTable: " + file.getAbsolutePath());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
