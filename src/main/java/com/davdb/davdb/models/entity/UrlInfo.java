package com.davdb.davdb.models.entity;

import com.davdb.davdb.models.dto.UrlEntryDTO;

import java.time.LocalDateTime;

public class UrlInfo {

    Long count;
    LocalDateTime timestamp;

    public UrlInfo(Long count, LocalDateTime timestamp) {
        this.count = count;
        this.timestamp = timestamp;
    }

    public static UrlInfo from(UrlEntryDTO dto) {
        return new UrlInfo(dto.getCount(), dto.getTimestamp());
    }

    @Override
    public String toString() {
        return "UrlInfo{\"count\":\""+this.count+"\", \"timestamp\":\""+this.timestamp+"\" }";
    }

    public Long getCount() {
        return count;
    }


    public LocalDateTime getTimestamp() {
        return timestamp;
    }

}
