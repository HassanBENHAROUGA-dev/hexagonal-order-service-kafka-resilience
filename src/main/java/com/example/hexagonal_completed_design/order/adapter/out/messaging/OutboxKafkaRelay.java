package com.example.hexagonal_completed_design.order.adapter.out.messaging;

import com.example.hexagonal_completed_design.order.adapter.out.persistance.entity.OutboxEntity;
import com.example.hexagonal_completed_design.order.adapter.out.persistance.jpa.SpringDataOutboxRepository;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@Component
public class OutboxKafkaRelay {

    private static final String TOPIC = "order-events-secure";
    private static final String EVENT_ID_HEADER = "eventId";

    private final SpringDataOutboxRepository outboxRepository;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public OutboxKafkaRelay(
            SpringDataOutboxRepository outboxRepository,
            KafkaTemplate<Object, Object> kafkaTemplate
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    /*
     * Polls the Outbox periodically and publishes pending events to Kafka.
     *
     * An event remains unprocessed when Kafka publication fails,
     * allowing a later execution of the relay to retry it.
     */
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void relayEventsToKafka() {

        List<OutboxEntity> pendingEvents =
                outboxRepository.findByProcessedFalse();

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.debug(
                "Found {} pending Outbox event(s) to publish",
                pendingEvents.size()
        );

        for (OutboxEntity entity : pendingEvents) {
            publishEvent(entity);
        }
    }

    private void publishEvent(OutboxEntity entity) {

        try {
            ProducerRecord<Object, Object> record = new ProducerRecord<>(
                    TOPIC,
                    entity.getAggregateId(),
                    entity.getPayload()
            );

            record.headers().add(
                    EVENT_ID_HEADER,
                    entity.getId()
                            .toString()
                            .getBytes(StandardCharsets.UTF_8)
            );

            log.info(
                    "➡️ Trying to publish Outbox event | eventId={} | aggregateId={}",
                    entity.getId(),
                    entity.getAggregateId()
            );

            kafkaTemplate.send(record).get();

            entity.setProcessed(true);
            outboxRepository.save(entity);

            log.info(
                    "✅ OUTBOX EVENT PUBLISHED | eventId={} | aggregateId={} | eventType={}",
                    entity.getId(),
                    entity.getAggregateId(),
                    entity.getEventType()
            );

        } catch (Exception e) {

            log.error(
                    "❌ OUTBOX PUBLICATION FAILED | eventId={} | aggregateId={} | eventType={} | exception={} | message={} | retry=next-poll",
                    entity.getId(),
                    entity.getAggregateId(),
                    entity.getEventType(),
                    e.getClass().getSimpleName(),
                    e.getMessage()
            );
        }
    }
}