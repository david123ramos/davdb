package com.davdb.davdb.infra.persistance;

import com.davdb.davdb.infra.persistance.serialization.Serializer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class SSTableReader<K, V> {

    Serializer<K> keySerializer;
    Serializer<V> valueSerializer;


    public SSTableReader(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }


    public SStable<K, V> readMostRecent() {
        File file = Path.of(SStable.ROOT_DATA_PATH+"/CURRENT.txt").toFile();

        if(!file.exists()) return null;

        String sstablename;

        try(DataInputStream in = new DataInputStream(new FileInputStream(file))) {

            while (in.available() > 0) {
                int len = in.readInt();
                byte[] nameBytes = in.readNBytes(len);
                sstablename = new String(nameBytes, StandardCharsets.UTF_8);

                String finalName = new StringBuilder()
                        .append(SStable.ROOT_DATA_PATH)
                        .append("/")
                        .append(sstablename)
                        .toString();

                return new SStable(finalName, keySerializer, valueSerializer);
            }

        }catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }
}
