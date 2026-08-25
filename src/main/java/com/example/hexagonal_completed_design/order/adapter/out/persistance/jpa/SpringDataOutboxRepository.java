package com.example.hexagonal_completed_design.order.adapter.out.persistance.jpa;

import com.example.hexagonal_completed_design.order.adapter.out.persistance.entity.OutboxEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface SpringDataOutboxRepository extends JpaRepository<OutboxEntity, UUID> {
    // Permet au Relay de récupérer uniquement les événements non envoyés
    List<OutboxEntity> findByProcessedFalseOrderByCreatedAtAsc();
    List<OutboxEntity> findByProcessedFalse();
}