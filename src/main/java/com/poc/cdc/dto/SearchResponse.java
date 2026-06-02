package com.poc.cdc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SearchResponse<T> {
    private String engine;      // "postgresql" or "elasticsearch"
    private String query;
    private long totalHits;     // total matching records
    private long tookMs;        // wall-clock time for the query
    private List<T> results;    // first 10 results
}
