package com.example.hexagonal_completed_design.order.domain.event;

import com.example.hexagonal_completed_design.order.domain.domainEvent.DomainEvent;
import com.example.hexagonal_completed_design.order.domain.valueobject.OrderId;

import java.time.Instant;

public record OrderShippedEvent(OrderId orderId, String trackingNumber, Instant occurredOn) implements DomainEvent {
    @Override
    public String getAggregateId() {
        return orderId().value().toString();
    }
}
