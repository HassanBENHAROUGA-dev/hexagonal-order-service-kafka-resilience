package com.example.hexagonal_completed_design.order.adapter.in.messaging;

import com.example.hexagonal_completed_design.order.domain.event.OrderConfirmedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    // 🌟 On écoute avec le groupe 3, et on demande l'objet Kafka BRUT
    @KafkaListener(topics = "order-events", groupId = "shipping-group-3")
    public void consumeOrderEvent(ConsumerRecord<String, String> record) {

        System.out.println("🔥 BINGO ! MESSAGE REÇU DE KAFKA !");
        System.out.println("📍 Clé : " + record.key());
        System.out.println("📍 Valeur : " + record.value());

        // 💥 ON PROVOQUE L'ERREUR VOLONTAIRE
        throw new RuntimeException("Erreur critique test DLQ !");
    }

    // Le topic DLT pour la DLQ
    @KafkaListener(topics = "order-events.DLT", groupId = "shipping-dlq-group-3")
    public void consumeDLQ(ConsumerRecord<String, String> record) {

        System.err.println("🚨 MESSAGE DANS LA DLQ ! Le message " + record.key() + " a échoué 3 fois.");
    }
}