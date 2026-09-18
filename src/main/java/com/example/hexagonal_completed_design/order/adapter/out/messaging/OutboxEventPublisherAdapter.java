package com.example.hexagonal_completed_design.order.adapter.out.messaging;

import com.example.hexagonal_completed_design.order.adapter.out.persistance.entity.OutboxEntity;
import com.example.hexagonal_completed_design.order.adapter.out.persistance.jpa.SpringDataOutboxRepository;
import com.example.hexagonal_completed_design.order.application.port.out.EventPublisherPort;
import com.example.hexagonal_completed_design.order.domain.domainEvent.DomainEvent;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

@Component
@Primary // On indique à Spring d'utiliser cet adapter pour le EventPublisherPort
public class OutboxEventPublisherAdapter implements EventPublisherPort {

    private final SpringDataOutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OutboxEventPublisherAdapter(SpringDataOutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(DomainEvent event) {
        // Sérialisation du Domain Event en JSON
        String payload = objectMapper.writeValueAsString(event);

        // Création de la ligne en base de données
        OutboxEntity outboxEntity = new OutboxEntity(
                UUID.randomUUID(),
                event.getAggregateId(),// Pseudo-méthode pour récupérer l'ID
                event.getClass().getSimpleName(),
                payload,
                event.occurredOn()
        );

        // Sauvegarde dans la même transaction que l'Aggregate Order !
        outboxRepository.save(outboxEntity);

    }
}