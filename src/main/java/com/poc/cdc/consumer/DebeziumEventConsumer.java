package com.poc.cdc.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.poc.cdc.document.TicketDocument;
import com.poc.cdc.repository.TicketSearchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Consumes Debezium CDC events in batches and bulk-indexes into Elasticsearch.
 *
 * Batch mode (spring.kafka.listener.type=batch) gives us up to 500 records per
 * poll. One saveAll() call per batch = one HTTP round-trip to ES vs 500 individual
 * calls — roughly 100x faster for initial snapshot of 1M records.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class DebeziumEventConsumer {

    private final TicketSearchRepository searchRepository;

    @KafkaListener(topics = "dbserver1.public.tickets", groupId = "cdc-consumer-group")
    public void handle(List<ConsumerRecord<String, JsonNode>> records) {
        List<TicketDocument> toUpsert = new ArrayList<>(records.size());
        List<String> toDelete = new ArrayList<>();

        for (ConsumerRecord<String, JsonNode> record : records) {
            JsonNode event = record.value();
            if (event == null || !event.has("op")) continue;

            String op = event.get("op").asText();
            switch (op) {
                case "c", "u", "r" -> {
                    JsonNode after = event.get("after");
                    if (after != null && !after.isNull()) toUpsert.add(buildDocument(after));
                }
                case "d" -> {
                    JsonNode before = event.get("before");
                    if (before != null && !before.isNull()) toDelete.add(before.get("id").asText());
                }
            }
        }

        if (!toUpsert.isEmpty()) {
            searchRepository.saveAll(toUpsert);
            log.info("Batch indexed {} tickets (total in batch: {})", toUpsert.size(), records.size());
        }
        if (!toDelete.isEmpty()) {
            searchRepository.deleteAllById(toDelete);
            log.info("Batch deleted {} tickets from index", toDelete.size());
        }
    }

    private TicketDocument buildDocument(JsonNode after) {
        return TicketDocument.builder()
                .id(after.get("id").asText())
                .title(after.path("title").asText(null))
                .description(after.path("description").asText(null))
                .status(after.path("status").asText(null))
                .priority(after.path("priority").asText(null))
                .assignee(after.path("assignee").asText(null))
                .build();
    }
}
