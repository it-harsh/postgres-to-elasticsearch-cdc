package com.poc.cdc.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.poc.cdc.document.TicketDocument;
import com.poc.cdc.repository.TicketSearchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes Debezium CDC events from Kafka and keeps Elasticsearch in sync.
 *
 * Debezium event structure (schemas.enable=false):
 * {
 *   "op":     "c" | "u" | "d" | "r"   (create, update, delete, read/snapshot)
 *   "before": { ...row fields... }      (null for inserts)
 *   "after":  { ...row fields... }      (null for deletes)
 * }
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class DebeziumEventConsumer {

    private final TicketSearchRepository searchRepository;

    // Topic format: {topic.prefix}.{schema}.{table}  →  dbserver1.public.tickets
    @KafkaListener(topics = "dbserver1.public.tickets", groupId = "cdc-consumer-group")
    public void handle(ConsumerRecord<String, JsonNode> record) {
        try {
            JsonNode event = record.value();
            if (event == null || !event.has("op")) return;

            String op = event.get("op").asText();
            switch (op) {
                case "c", "u", "r" -> upsert(event.get("after"));
                case "d"           -> delete(event.get("before"));
                default            -> log.warn("Unknown CDC op: {}", op);
            }
        } catch (Exception e) {
            log.error("Failed to process CDC event at offset {}: {}", record.offset(), e.getMessage());
        }
    }

    private void upsert(JsonNode after) {
        if (after == null || after.isNull()) return;

        TicketDocument doc = TicketDocument.builder()
                .id(after.get("id").asText())
                .title(after.path("title").asText(null))
                .description(after.path("description").asText(null))
                .status(after.path("status").asText(null))
                .priority(after.path("priority").asText(null))
                .assignee(after.path("assignee").asText(null))
                .build();

        searchRepository.save(doc);
        log.debug("Indexed ticket {}", doc.getId());
    }

    private void delete(JsonNode before) {
        if (before == null || before.isNull()) return;
        String id = before.get("id").asText();
        searchRepository.deleteById(id);
        log.debug("Deleted ticket {} from index", id);
    }
}
