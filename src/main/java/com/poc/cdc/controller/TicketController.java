package com.poc.cdc.controller;

import com.poc.cdc.document.TicketDocument;
import com.poc.cdc.dto.SearchResponse;
import com.poc.cdc.dto.TicketRequest;
import com.poc.cdc.model.Ticket;
import com.poc.cdc.service.TicketService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/tickets")
@RequiredArgsConstructor
public class TicketController {

    private final TicketService ticketService;

    // Single-row write — goes to PostgreSQL, Debezium picks it up via WAL
    @PostMapping
    public ResponseEntity<Ticket> create(@RequestBody TicketRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(ticketService.create(request));
    }

    // Bulk inserts N rows via JDBC batch — runs async, returns immediately
    // Debezium snapshots these rows and publishes to Kafka → syncs to Elasticsearch
    @PostMapping("/generate")
    public ResponseEntity<Map<String, Object>> generate(
            @RequestParam(defaultValue = "1000000") int count) {
        ticketService.bulkGenerate(count);
        return ResponseEntity.accepted().body(Map.of(
                "message", "Generating " + count + " tickets in background via JDBC batch",
                "checkProgress", "GET /api/tickets/sync-status"
        ));
    }

    // SLOW: PostgreSQL sequential scan via LIKE — watch tookMs climb past 20,000ms on 1M rows
    @GetMapping("/search/postgres")
    public ResponseEntity<SearchResponse<Ticket>> searchPostgres(@RequestParam String q) {
        return ResponseEntity.ok(ticketService.searchPostgres(q));
    }

    // FAST: Elasticsearch inverted index — tookMs stays under 100ms regardless of volume
    @GetMapping("/search/elastic")
    public ResponseEntity<SearchResponse<TicketDocument>> searchElastic(@RequestParam String q) {
        return ResponseEntity.ok(ticketService.searchElastic(q));
    }

    // Shows how many rows Postgres has vs how many Elasticsearch has indexed (CDC lag)
    @GetMapping("/sync-status")
    public ResponseEntity<Map<String, Object>> syncStatus() {
        return ResponseEntity.ok(ticketService.syncStatus());
    }
}
