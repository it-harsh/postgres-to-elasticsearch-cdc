package com.poc.cdc.repository;

import com.poc.cdc.model.Ticket;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface TicketRepository extends JpaRepository<Ticket, Long> {

    // Intentionally uses LIKE with leading wildcard — forces a full sequential scan.
    // This is the slow path we're benchmarking against Elasticsearch.
    // On 1M rows this takes 20-60 seconds even on SSD.
    @Query(value = "SELECT * FROM tickets WHERE description LIKE '%' || :q || '%' OR title LIKE '%' || :q || '%' LIMIT 10",
           nativeQuery = true)
    List<Ticket> slowSearch(@Param("q") String q);

    @Query(value = "SELECT COUNT(*) FROM tickets WHERE description LIKE '%' || :q || '%' OR title LIKE '%' || :q || '%'",
           nativeQuery = true)
    long slowSearchCount(@Param("q") String q);
}
