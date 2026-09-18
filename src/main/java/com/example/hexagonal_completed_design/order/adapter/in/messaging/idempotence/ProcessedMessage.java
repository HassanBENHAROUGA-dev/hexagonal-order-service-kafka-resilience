package com.example.hexagonal_completed_design.order.adapter.in.messaging.idempotence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.LocalDateTime;

@Entity
@Table(name = "processed_messages")
public class ProcessedMessage {

    @Getter
    @Id
    @Column(name = "message_id", unique = true, nullable = false, updatable = false)
    private String messageId;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    public ProcessedMessage() {
    }

    public ProcessedMessage(String messageId) {
        this.messageId = messageId;
        this.processedAt = LocalDateTime.now();
    }

}
