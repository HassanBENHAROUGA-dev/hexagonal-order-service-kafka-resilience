package com.example.hexagonal_completed_design.unit;

import com.example.hexagonal_completed_design.order.adapter.out.persistance.jpa.JpaOrderPersistenceAdapter;
import com.example.hexagonal_completed_design.order.domain.aggregate.Order;
import com.example.hexagonal_completed_design.order.domain.valueobject.Money;
import com.example.hexagonal_completed_design.order.domain.valueobject.OrderId;
import com.example.hexagonal_completed_design.order.domain.valueobject.ProductId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
public class OrderConcurrencyIT {

    @Autowired
    private JpaOrderPersistenceAdapter orderPersistenceAdapter;

    @Test
    void should_throw_optimistic_locking_exception_on_concurrent_updates() {
        // 1. Préparation : On crée une commande métier et on la sauvegarde
        OrderId orderId = new OrderId(UUID.randomUUID());
        Order initialOrder = Order.create(orderId);
        // Note : Ajoute un item si ton domaine exige qu'une commande ne soit pas vide pour la confirmer
        initialOrder.addItem(new ProductId(UUID.randomUUID()), 1, new Money(BigDecimal.valueOf(100), "EUR"));

        orderPersistenceAdapter.save(initialOrder); // Sauvegarde initiale (Version = 0)

        // 2. Le Thread A charge la commande (Le Mapper restaure avec la Version = 0)
        // Remplace getOrder par le nom de ta méthode dans l'adapter (ex: findById)
        Order threadA_Order = orderPersistenceAdapter.findById(orderId).orElseThrow();

        // 3. Le Thread B charge EXACTEMENT la même commande en même temps (Version = 0)
        Order threadB_Order = orderPersistenceAdapter.findById(orderId).orElseThrow();

        // 4. Le Thread A modifie la commande (Action métier) et sauvegarde
        threadA_Order.confirm();
        orderPersistenceAdapter.save(threadA_Order); // SUCCÈS : En base, la version passe à 1

        // 5. Le Thread B modifie SA version de la commande (Action métier)
        threadB_Order.cancel();

        // 6. 💥 VERDICT : Le Mapper du Thread B va envoyer la version 0 à JPA.
        // Hibernate va voir que la base est déjà à la version 1 et va rejeter la transaction !
        assertThrows(ObjectOptimisticLockingFailureException.class, () -> {
            orderPersistenceAdapter.save(threadB_Order);
        });
    }
}
