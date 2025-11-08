package com.davdb.davdb.services;

import com.davdb.davdb.infra.persistance.Memtable;
import com.davdb.davdb.infra.persistance.SSTableReader;
import com.davdb.davdb.infra.persistance.SStable;
import com.davdb.davdb.infra.persistance.serialization.Serializer;
import com.davdb.davdb.models.dto.UrlEntryDTO;
import com.davdb.davdb.models.entity.UrlInfo;
import com.davdb.davdb.models.entity.Url;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Service
public class UrlService {

    private Memtable<String, UrlInfo> memtable;
    SSTableReader<String, UrlInfo> tableReader;


    UrlService(Serializer<String> keySerializer, Serializer<UrlInfo> urlInfoSerializer) {
        this.memtable = new Memtable<>(keySerializer, urlInfoSerializer);
        this.tableReader = new SSTableReader<>(keySerializer, urlInfoSerializer);
    }

    public void saveUrlClick(UrlEntryDTO entry) throws Exception {
        System.out.println("[INFO] Received: "+entry);
        Url entity = new Url( entry.getUrl(), UrlInfo.from(entry));

        try {
            memtable.insert(entity);
        }catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

    public SortedMap<String, UrlInfo> readLast() {
        SStable<String, UrlInfo> tb = this.tableReader.readMostRecent();
        return Collections.unmodifiableSortedMap(tb != null ? tb.getMap() : new ConcurrentSkipListMap<>());
    }

}
