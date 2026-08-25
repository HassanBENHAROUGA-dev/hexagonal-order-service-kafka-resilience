/*
package com.example.hexagonal_completed_design.order.adapter.out.messaging;

import com.example.hexagonal_completed_design.order.application.port.out.EventPublisherPort;
import com.example.hexagonal_completed_design.order.domain.domainEvent.DomainEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventPublisher implements EventPublisherPort { // Ton port existant

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private static final String TOPIC = "order-events";

    public KafkaEventPublisher(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // On utilise bien le type générique de l'interface !
    @Override
    public void publish(DomainEvent event) {

        // Grâce à ton interface, ça marche pour TOUS tes événements !
        String key = event.getAggregateId();

        kafkaTemplate.send(TOPIC, key, event);
        System.out.println("🚀 Événement publié dans Kafka : " + event.getClass().getSimpleName() + " avec la clé : " + key);
    }
}*/
