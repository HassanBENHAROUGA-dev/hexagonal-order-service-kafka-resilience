package com.example.hexagonal_completed_design.order.domain.domainEvent;

import java.time.Instant;

public interface DomainEvent {
    Instant occurredOn();
    String getAggregateId();
}