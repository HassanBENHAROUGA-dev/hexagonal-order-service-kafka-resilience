package com.example.hexagonal_completed_design.order.infrastructure.config.messaging;

import com.example.hexagonal_completed_design.order.adapter.out.persistance.entity.OutboxEntity;
import com.example.hexagonal_completed_design.order.adapter.out.persistance.jpa.SpringDataOutboxRepository;
import jakarta.transaction.Transactional;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/*
@Component
public class OutboxRelayScheduler {

    private final SpringDataOutboxRepository outboxRepository;

    public OutboxRelayScheduler(SpringDataOutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    // S'exécute toutes les 5 secondes (pour l'exemple)
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void processOutbox() {
        List<OutboxEntity> pendingEvents = outboxRepository.findByProcessedFalseOrderByCreatedAtAsc();

        for (OutboxEntity event : pendingEvents) {
            try {
                // ICI : Tu enverrais le payload JSON vers ton vrai Broker Kafka/RabbitMQ
                System.out.println("🚀 [OUTBOX RELAY] Sending to Broker: " + event.getPayload());

                // Une fois envoyé avec succès, on le marque comme traité
                event.setProcessed(true);
                outboxRepository.save(event);

            } catch (Exception e) {
                System.err.println("Failed to process outbox event: " + event.getId());
                // On ne bloque pas la boucle, on retentera au prochain passage
            }
        }
    }
}*/
