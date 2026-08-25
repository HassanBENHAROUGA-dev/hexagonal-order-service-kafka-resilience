package com.example.hexagonal_completed_design.order.adapter.out.messaging;

import com.example.hexagonal_completed_design.order.adapter.out.persistance.entity.OutboxEntity;
import com.example.hexagonal_completed_design.order.adapter.out.persistance.jpa.SpringDataOutboxRepository;
import jakarta.transaction.Transactional;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OutboxKafkaRelay {

    private final SpringDataOutboxRepository outboxRepository;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    private static final String TOPIC = "order-events-secure";

    public OutboxKafkaRelay(SpringDataOutboxRepository outboxRepository, KafkaTemplate<Object, Object> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    // 🌟 S'exécute toutes les 5000 millisecondes (5 secondes)
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void relayEventsToKafka() {

        // 1. Lire tous les événements en attente dans la table Outbox
        List<OutboxEntity> pendingEvents = outboxRepository.findByProcessedFalse();

        if (pendingEvents.isEmpty()) {
            return; // Rien à faire, on se rendort 😴
        }

        System.out.println("🚂 Relais Outbox : " + pendingEvents.size() + " message(s) trouvé(s) ! Envoi vers Kafka...");

        for (OutboxEntity entity : pendingEvents) {
            try {
                // 2. Envoyer le contenu brut (JSON) vers Kafka
                // (Adapte .getAggregateId() et .getPayload() selon le nom exact de tes getters dans OutboxEntity)
                // 3. Si l'envoi réussit, on supprime la ligne de la BDD
                //outboxRepository.delete(entity);
                kafkaTemplate.send(TOPIC, String.valueOf(entity.getId()), entity.getPayload()).get();
                entity.setEventType("Published"); // Ce que tu as fait (très bien pour le log)
                entity.setProcessed(true);        // 🌟 INDISPENSABLE pour que la BDD l'ignore au prochain tour

                outboxRepository.save(entity);

                //System.out.println("✅ Message envoyé et supprimé de l'Outbox : " + entity.getId());
                System.out.println("✅ Message envoyé et traité de l'Outbox : " + entity.getId());

            } catch (Exception e) {
                System.err.println("❌ Échec de l'envoi pour l'événement " + entity.getId() + ". On réessaiera au prochain tour.");
                // On ne supprime pas l'entité, le poller réessaiera dans 5 secondes !
                e.printStackTrace();
            }
        }
    }
}