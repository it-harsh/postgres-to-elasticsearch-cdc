package com.poc.cdc.document;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * Elasticsearch representation of a ticket.
 *
 * Field types matter for search:
 *   Text   → tokenized + inverted index (full-text search, stemming, fuzzy)
 *   Keyword → exact match only (filtering, aggregations, sorting)
 */
@Document(indexName = "tickets")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TicketDocument {

    @Id
    private String id;

    // "english" analyzer: lowercases, removes stopwords, applies stemming
    // e.g. "database errors" → tokens: ["databas", "error"] → fuzzy-matchable
    @Field(type = FieldType.Text, analyzer = "english")
    private String title;

    @Field(type = FieldType.Text, analyzer = "english")
    private String description;

    // Keyword = exact match — used for status/priority filters and aggregations
    @Field(type = FieldType.Keyword)
    private String status;

    @Field(type = FieldType.Keyword)
    private String priority;

    @Field(type = FieldType.Keyword)
    private String assignee;
}
