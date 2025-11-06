package com.davdb.davdb.models.dto;

import java.time.LocalDateTime;

public class UrlEntryDTO {

    private final String url;
    private final Long count;
    private final LocalDateTime timestamp;

    public UrlEntryDTO(String url, Long count) {
        this.url = url;
        this.count = count;
        this.timestamp = LocalDateTime.now();
    }

    @Override
    public String toString() {
        return "{ \"url\" : \""+this.url+"\", \"count\":\""+this.count+"\", \"timestamp\":\""+this.timestamp+"\" }";
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public Long getCount() {
        return count;
    }

    public String getUrl() {
        return url;
    }
}