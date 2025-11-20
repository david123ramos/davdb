package com.davdb.infra.persistance;

import com.davdb.infra.persistance.serialization.Serializer;

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

    /*
    * |================SSTABLE==============|
    * |---------HEADER----------------------|
    * | MAGIC_NUMBER: 0xF1A5C0DB (fiasco db)|
    * |-------------------------------------|
    * |DATA BLOCKS -------------------------|
    * |      |BLOCK 1---------------------| |
    * |      |    key1 - value1           | |
    * |      |    key2 - value2           | |
    * |      |    key3 - value3           | |
    * |      |----------------------------| |
    * |      | block1 checksum            | |
    * |      |----------------------------| |
    * |      |BLOCK N---------------------| |
    * |      |    keyN - valueN           | |
    * |      |    keyN2 - valueN2         | |
    * |      |    keyN3 - valueN3         | |
    * |      |----------------------------| |
    * |      | block1 checksum            | |
    * |      |----------------------------| |
    * |-------------------------------------|
    * |INDEX -------------------------------|
    * | key1 @ offset 4931 @ block 1        |
    * | keyN @ offset 8741 @ block N        |
    * | index checksum                      |
    * |-------------------------------------|
    * |Bloom FILTER ------------------------|
    * | entries: [1,2,4,5...N]              |
    * | bucket: 3                           |
    * | finger: 3bits                       |
    * |-------------------------------------|
    * |Footer-------------------------------|
    * |index @ offset 1023912               |
    * |index_size: 1231231344               |
    * |Bloom_filter @ offset 1209483        |
    * |Bloom_filter_size: 19234             |
    * |MAGIC_NUMBER: 0xF1A5C0DBE7A11 (fiasco db tail)
    * |=====================================|
    * */
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
