package com.davdb.davdb.services;

import com.davdb.davdb.infra.persistance.Memtable;
import com.davdb.davdb.infra.persistance.serialization.Serializer;
import com.davdb.davdb.models.dto.UrlEntryDTO;
import com.davdb.davdb.models.entity.UrlInfo;
import com.davdb.davdb.models.entity.Url;
import org.springframework.stereotype.Service;

@Service
public class UrlService {

    private Memtable<String, UrlInfo> memtable;

    UrlService(Serializer<String> keySerializer, Serializer<UrlInfo> urlInfoSerializer) {
        this.memtable = new Memtable<>(keySerializer, urlInfoSerializer);
    }

    public void saveUrlClick(UrlEntryDTO entry) {
        System.out.println("[INFO] Received: "+entry);
        Url entity = new Url( entry.getUrl(), UrlInfo.from(entry));

        try {
            memtable.insert(entity);
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void printTable() {
        this.memtable.printdata();
    }

}
