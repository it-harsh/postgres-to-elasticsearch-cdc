package com.poc.cdc.service;

import com.poc.cdc.document.TicketDocument;
import com.poc.cdc.dto.SearchResponse;
import com.poc.cdc.dto.TicketRequest;
import com.poc.cdc.model.Ticket;
import com.poc.cdc.repository.TicketRepository;
import com.poc.cdc.repository.TicketSearchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class TicketService {

    private final TicketRepository ticketRepository;
    private final TicketSearchRepository ticketSearchRepository;
    private final JdbcTemplate jdbcTemplate;

    private static final String[] STATUSES   = {"OPEN", "IN_PROGRESS", "CLOSED"};
    private static final String[] PRIORITIES = {"LOW", "MEDIUM", "HIGH", "CRITICAL"};
    private static final String[] MODULES    = {"auth", "payment", "database", "network", "cache", "api", "scheduler", "worker"};
    private static final String[] ERRORS     = {"timeout", "connection refused", "null pointer", "memory leak", "deadlock", "race condition"};

    public Ticket create(TicketRequest req) {
        Ticket ticket = Ticket.builder()
                .title(req.getTitle())
                .description(req.getDescription())
                .status("OPEN")
                .priority(req.getPriority())
                .assignee(req.getAssignee())
                .build();
        return ticketRepository.save(ticket);
    }

    // Runs in background so the HTTP call returns immediately.
    // JdbcTemplate.batchUpdate inserts 500 rows per round trip — ~10x faster than JPA save().
    @Async
    public void bulkGenerate(int count) {
        log.info("Starting bulk generation of {} tickets...", count);
        String sql = """
                INSERT INTO tickets (title, description, status, priority, assignee, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, NOW(), NOW())
                """;

        int batchSize = 500;
        List<Object[]> batch = new ArrayList<>(batchSize);

        for (int i = 1; i <= count; i++) {
            String module = MODULES[i % MODULES.length];
            String error  = ERRORS[i % ERRORS.length];
            batch.add(new Object[]{
                    "Ticket #" + i + " [" + PRIORITIES[i % PRIORITIES.length] + "]",
                    "Critical " + error + " detected in " + module + " module. " +
                    "Reported at " + LocalDateTime.now() + ". Assigned to user" + (i % 50),
                    STATUSES[i % STATUSES.length],
                    PRIORITIES[i % PRIORITIES.length],
                    "user" + (i % 50) + "@company.com"
            });

            if (batch.size() == batchSize) {
                jdbcTemplate.batchUpdate(sql, batch);
                batch.clear();
                if (i % 50_000 == 0) {
                    log.info("Progress: {}/{} tickets inserted", i, count);
                }
            }
        }
        if (!batch.isEmpty()) {
            jdbcTemplate.batchUpdate(sql, batch);
        }
        log.info("Bulk generation complete: {} tickets written to PostgreSQL. CDC syncing to Elasticsearch...", count);
    }

    // Slow path — raw LIKE on PostgreSQL. Demonstrates full sequential scan.
    public SearchResponse<Ticket> searchPostgres(String query) {
        long start = System.currentTimeMillis();
        List<Ticket> results = ticketRepository.slowSearch(query);
        long totalHits = ticketRepository.slowSearchCount(query);
        long tookMs = System.currentTimeMillis() - start;

        return SearchResponse.<Ticket>builder()
                .engine("postgresql")
                .query(query)
                .totalHits(totalHits)
                .tookMs(tookMs)
                .results(results)
                .build();
    }

    // Fast path — inverted index via Elasticsearch multi_match.
    public SearchResponse<TicketDocument> searchElastic(String query) {
        long start = System.currentTimeMillis();
        Page<TicketDocument> page = ticketSearchRepository.fullTextSearch(query, PageRequest.of(0, 10));
        long tookMs = System.currentTimeMillis() - start;

        return SearchResponse.<TicketDocument>builder()
                .engine("elasticsearch")
                .query(query)
                .totalHits(page.getTotalElements())
                .tookMs(tookMs)
                .results(page.getContent())
                .build();
    }

    public Map<String, Object> syncStatus() {
        long pgCount = ticketRepository.count();
        long esCount = ticketSearchRepository.count();
        double pct = pgCount > 0 ? (esCount * 100.0 / pgCount) : 0;
        return Map.of(
                "postgresql", pgCount,
                "elasticsearch", esCount,
                "lag", pgCount - esCount,
                "syncPercent", String.format("%.1f%%", pct)
        );
    }
}
