package com.davdb.davdb.config;


import com.davdb.davdb.infra.persistance.serialization.Serializer;
import org.springframework.stereotype.Component;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Component
public class StringSerializer implements Serializer<String> {

    @Override
    public void write(String value, DataOutputStream out) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public String read(DataInputStream in) throws IOException {
        int sz = in.readInt();
        byte[] bytes = in.readNBytes(sz);

        return new String(bytes, StandardCharsets.UTF_8);
    }
}
