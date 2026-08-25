package com.example.hexagonal_completed_design.order.domain.event;

import com.example.hexagonal_completed_design.order.domain.domainEvent.DomainEvent;
import com.example.hexagonal_completed_design.order.domain.valueobject.OrderId;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderDiscountedEvent(OrderId orderId, String codePromo, BigDecimal discount, Instant occurredOn) implements DomainEvent {
    @Override
    public String getAggregateId() {
        return orderId().value().toString();
    }
}
