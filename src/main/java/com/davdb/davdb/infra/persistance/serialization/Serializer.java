package com.davdb.davdb.infra.persistance.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Serializer<T> {
    void write(T value, DataOutputStream out) throws IOException;
    T read(DataInputStream in) throws IOException;
}