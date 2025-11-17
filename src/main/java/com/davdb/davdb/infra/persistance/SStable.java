package com.davdb.davdb.infra.persistance;

import com.davdb.davdb.infra.persistance.serialization.Serializer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class SStable<K,V> {
    private final SortedMap<K,V> map;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    public final static String ROOT_DATA_PATH = "data/";

    public SStable(String path, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws Exception {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.map = readFromDisk(path);
    }

    public SStable(SortedMap<K, V> map, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.map = map;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public void writeToFile() {
        System.out.println("[SSTable] Saving memtable in sstable file");
        String tablename = "table/sstable_"+ LocalDateTime.now().toInstant(ZoneOffset.UTC)+".sst";

        File file = new File(ROOT_DATA_PATH + tablename);

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

        updateCurrentFile(tablename);
    }

    private SortedMap<K,V> readFromDisk(String sstablePath) throws Exception {

        File file = new File(sstablePath);
        SortedMap<K, V> result = new TreeMap();

        if(!file.exists()) throw new FileNotFoundException("[SSTable] No SSTable with "+sstablePath+" was found");

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));

        while (dis.available() > 0) {
            K key = keySerializer.read(dis);
            V value = valueSerializer.read(dis);

            result.put(key, value);
        }

        return result;
    }

    private void updateCurrentFile(String tablename) {
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(Path.of(ROOT_DATA_PATH+"/CURRENT.txt").toFile())))) {
            byte[] bytes = tablename.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(bytes.length);
            dos.write(bytes);
            dos.flush();

        }catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public SortedMap<K, V> getMap() {
        return map;
    }
}
