package com.example.hexagonal_completed_design.order.adapter.out.persistance.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
public class OutboxEntity {

    // Getters et Setters
    @Getter
    @Id
    private UUID id;

    @Getter
    private String aggregateId;

    @Getter @Setter
    private String eventType;

    @Getter
    @Column(columnDefinition = "TEXT")
    private String payload; // L'événement sérialisé en JSON

    private Instant createdAt;
    @Setter
    @Getter
    private boolean processed;

    protected OutboxEntity() {}

    public OutboxEntity(UUID id, String aggregateId, String eventType, String payload, Instant createdAt) {
        this.id = id;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.createdAt = createdAt;
        this.processed = false;
    }

}