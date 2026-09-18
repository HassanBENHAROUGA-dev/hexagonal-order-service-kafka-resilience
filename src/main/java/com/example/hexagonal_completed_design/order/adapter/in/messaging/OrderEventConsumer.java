package com.example.hexagonal_completed_design.order.adapter.in.messaging;

import com.example.hexagonal_completed_design.order.adapter.in.messaging.idempotence.IdempotentMessageProcessor;
import com.example.hexagonal_completed_design.order.infrastructure.exception.MissingEventIdException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private static final String EVENT_ID_HEADER = "eventId";

    private final IdempotentMessageProcessor idempotentProcessor;

    public OrderEventConsumer(IdempotentMessageProcessor idempotentProcessor) {
        this.idempotentProcessor = idempotentProcessor;
    }

    @KafkaListener(
            topics = "order-events-secure",
            groupId = "shipping-group"
    )
    public void consume(ConsumerRecord<String, String> record) {

        Header eventIdHeader = record.headers().lastHeader(EVENT_ID_HEADER);

        if (eventIdHeader == null) {
            throw new MissingEventIdException(
                    "Missing required Kafka header: " + EVENT_ID_HEADER
            );
        }
        if (record.value().contains("99999999-9999-9999-9999-999999999999")) {
            System.out.println("💥 Erreur volontaire pour tester Retry + DLT");
            throw new RuntimeException("Test volontaire DLT");
        }

        String eventId = new String(
                eventIdHeader.value(),
                StandardCharsets.UTF_8
        );

        String aggregateId = record.key();
        String payload = record.value();

        try {
            /*
             * The eventId is used as the idempotency key.
             * Business processing is executed only if this event
             * has not already been processed.
             */
            idempotentProcessor.process(
                    eventId,
                    payload,
                    () -> processBusinessEvent(payload)
            );

            log.info(
                    "Order event processed successfully " +
                            "[eventId={}, aggregateId={}, partition={}, offset={}]",
                    eventId,
                    aggregateId,
                    record.partition(),
                    record.offset()
            );

        } catch (DataIntegrityViolationException e) {
            /*
             * A duplicate event is considered already processed.
             * The exception is intentionally not rethrown to prevent
             * unnecessary Kafka retries or routing to the DLT.
             */
            log.warn(
                    "Duplicate order event ignored " +
                            "[eventId={}, aggregateId={}, partition={}, offset={}]",
                    eventId,
                    aggregateId,
                    record.partition(),
                    record.offset()
            );
        }
    }

    private void processBusinessEvent(String payload) {
        // Business processing will be implemented here.
        log.debug("Processing order event payload: {}", payload);
    }
}