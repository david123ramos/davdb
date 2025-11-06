package com.davdb.davdb.infra.persistance.entity;

public interface Entry<K,V> {
   K getkey();
   V getValue();
}
