package com.davdb.davdb.controllers;


import com.davdb.davdb.models.dto.UrlEntryDTO;
import com.davdb.davdb.models.entity.UrlInfo;
import com.davdb.davdb.services.UrlService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.SortedMap;

@RestController
@RequestMapping("/v1/url")
public class UrlController {

    private final UrlService urlService;

    public UrlController(UrlService urlService) {
        this.urlService = urlService;
    }

    @GetMapping(value = "/health", produces = "application/json")
    public String health() {
        return "{\"status\": \"beating\", \"tmstp\":\""+ LocalDateTime.now() +"\"}";
    }

    @PostMapping
    public ResponseEntity<String> saveUrl(@RequestBody UrlEntryDTO entry) {
        this.urlService.saveUrlClick(entry);
        return ResponseEntity.ok("👍");
    }

    @GetMapping
    public ResponseEntity<SortedMap<String, UrlInfo>> readLast(){
        return ResponseEntity.ok(this.urlService.readLast());
    }

}
