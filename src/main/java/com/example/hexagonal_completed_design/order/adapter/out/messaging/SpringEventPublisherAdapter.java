package com.example.hexagonal_completed_design.order.adapter.out.messaging;

import com.example.hexagonal_completed_design.order.application.port.out.EventPublisherPort;
import com.example.hexagonal_completed_design.order.domain.domainEvent.DomainEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/*
@Component
public class SpringEventPublisherAdapter implements EventPublisherPort {

    private final ApplicationEventPublisher publisher;

    public SpringEventPublisherAdapter(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void publish(DomainEvent event) {
        // Publie l'événement métier dans le bus Spring
        publisher.publishEvent(event);
        System.out.println("Published event: " + event.getClass().getSimpleName());
    }
}*/
