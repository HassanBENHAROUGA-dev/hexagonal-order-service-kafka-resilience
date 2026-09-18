package com.example.hexagonal_completed_design.order.adapter.in.messaging.idempotence;

import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class IdempotentMessageProcessor {

    private final ProcessedMessageRepository repository;

    public IdempotentMessageProcessor(ProcessedMessageRepository repository) {
        this.repository = repository;
    }

    // 🌟 La transaction est cruciale ici !
    @Transactional
    public void process(String messageId, String payload, Runnable businessLogic) {

        // 1. On tente d'insérer l'ID en base de données.
        // saveAndFlush force l'écriture immédiate pour déclencher l'erreur SQL s'il y a un doublon.
        repository.insertIdempotent(messageId, LocalDateTime.now());

        // 2. Si ça passe (pas d'exception lancée), on a le feu vert pour exécuter le métier !
        businessLogic.run();
    }
}