package com.example.hexagonal_completed_design.order.infrastructure.exception;

public class MissingEventIdException extends RuntimeException {
    public MissingEventIdException(String message) {
        super(message);
    }
}
