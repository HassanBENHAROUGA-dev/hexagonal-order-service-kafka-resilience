package com.example.hexagonal_completed_design.order.adapter.in.messaging.idempotence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
    // 🌟 ON FORCE L'INSERTION SQL POUR DÉCLENCHER L'ERREUR DE CLÉ PRIMAIRE
    @Modifying
    @Query(value = "INSERT INTO processed_messages (message_id, processed_at) VALUES (:messageId, :processedAt)", nativeQuery = true)
    void insertIdempotent(@Param("messageId") String messageId, @Param("processedAt") LocalDateTime processedAt);

}