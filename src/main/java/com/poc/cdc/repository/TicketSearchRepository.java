package com.poc.cdc.repository;

import com.poc.cdc.document.TicketDocument;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface TicketSearchRepository extends ElasticsearchRepository<TicketDocument, String> {

    // multi_match searches both title (2x boost) and description.
    // fuzziness:AUTO handles typos (e.g. "databse" matches "database").
    // Elasticsearch tokenizes, stems, and scores — all O(log n) via inverted index.
    @Query("""
            {
              "multi_match": {
                "query": "?0",
                "fields": ["title^2", "description"],
                "fuzziness": "AUTO",
                "type": "best_fields"
              }
            }
            """)
    Page<TicketDocument> fullTextSearch(String query, Pageable pageable);

    List<TicketDocument> findByStatus(String status);

    List<TicketDocument> findByPriority(String priority);
}
