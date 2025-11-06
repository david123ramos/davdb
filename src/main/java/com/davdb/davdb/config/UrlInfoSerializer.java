package com.davdb.davdb.config;

import com.davdb.davdb.infra.persistance.serialization.Serializer;
import com.davdb.davdb.models.entity.UrlInfo;
import org.springframework.stereotype.Component;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Component
public class UrlInfoSerializer implements Serializer<UrlInfo> {

    @Override
    public void write(UrlInfo value, DataOutputStream out) throws IOException {
        out.writeLong(value.getCount());
        out.writeLong(value.getTimestamp().toEpochSecond(ZoneOffset.UTC));
        out.writeInt(value.getTimestamp().getNano());
    }

    @Override
    public UrlInfo read(DataInputStream in) throws IOException {

        Long count  = in.readLong();
        Long timestamp = in.readLong();
        int nano = in.readInt();
        LocalDateTime createdAt = LocalDateTime.ofEpochSecond(timestamp,nano, ZoneOffset.UTC);

        return new UrlInfo(count, createdAt);
    }
}
