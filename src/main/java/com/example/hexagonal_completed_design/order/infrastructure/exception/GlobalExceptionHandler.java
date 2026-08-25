package com.example.hexagonal_completed_design.order.infrastructure.exception;

import com.example.hexagonal_completed_design.order.domain.exception.OrderBusinessException;
import com.example.hexagonal_completed_design.order.domain.exception.PromoCodeException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.time.Instant;

@RestControllerAdvice
public class GlobalExceptionHandler {

    // On intercepte uniquement NOTRE exception métier pure
    @ExceptionHandler(OrderBusinessException.class)
    public ProblemDetail handleOrderBusinessException(OrderBusinessException ex) {

        // On crée une réponse standardisée RFC 7807
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.UNPROCESSABLE_ENTITY, // HTTP 422
                ex.getMessage()
        );

        problemDetail.setTitle("Business Rule Violation");
        problemDetail.setType(URI.create("https://api.company.com/errors/business-rule-violation"));
        problemDetail.setProperty("timestamp", Instant.now());

        return problemDetail;
    }

    @ExceptionHandler(PromoCodeException.class)
    public ProblemDetail handlePromoCodeException(PromoCodeException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.NOT_FOUND,
                ex.getMessage()
        );

        problemDetail.setTitle("Business Rule Violation");
        problemDetail.setType(URI.create("https://api.company.com/errors/business-rule-violation"));
        problemDetail.setProperty("timestamp", Instant.now());

        return problemDetail;
    }

    // Optionnel : Intercepter les IllegalArgumentException de nos Value Objects (ex: Money négatif)
    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgumentException(IllegalArgumentException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, // HTTP 400
                ex.getMessage()
        );
        problemDetail.setTitle("Invalid Input");
        return problemDetail;
    }

    @ExceptionHandler(ObjectOptimisticLockingFailureException.class)
    public ProblemDetail handleOptimisticLocking(ObjectOptimisticLockingFailureException ex) {
        ProblemDetail problemDetail = ProblemDetail.forStatusAndDetail(
                HttpStatus.CONFLICT, // HTTP 409
                "La commande a été modifiée par un autre processus. Veuillez rafraîchir et réessayer."
        );
        problemDetail.setTitle("Concurrency Conflict");
        problemDetail.setProperty("timestamp", Instant.now());
        return problemDetail;
    }
}