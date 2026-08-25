package com.example.hexagonal_completed_design.order.domain.aggregate;

import com.example.hexagonal_completed_design.order.domain.aggregateRoot.AggregateRoot;
import com.example.hexagonal_completed_design.order.domain.event.*;
import com.example.hexagonal_completed_design.order.domain.exception.OrderBusinessException;
import com.example.hexagonal_completed_design.order.domain.valueobject.*;
import lombok.Getter;
import org.jspecify.annotations.NonNull;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class Order extends AggregateRoot<OrderId> {
    private final List<OrderItem> items;
    private OrderStatus status;

    @Getter
    private Discount discount;

    @Getter
    private Long version;
    // Constructeur privé : on force l'utilisation de méthodes de création métier
    private Order(OrderId id) {
        super(id);
        this.items = new ArrayList<>();
        this.status = OrderStatus.PENDING;
        this.discount = null; // Par défaut, pas de réduction
    }

    // Static Factory Method
    public static Order create(OrderId id) {
        Order order = new Order(id);
        order.registerEvent(new OrderCreatedEvent(id, Instant.now()));
        return order;
    }

    public void addItem(ProductId productId, int quantity, Money unitPrice) {
        checkMutability();

        Optional<OrderItem> existingItem = items.stream()
                .filter(item -> item.getProductId().equals(productId))
                .findFirst();

        if (existingItem.isPresent()) {
            existingItem.get().addQuantity(quantity);
        } else {
            this.items.add(new OrderItem(productId, quantity, unitPrice));
        }
    }

    public void removeItem(ProductId productId) {
        checkMutability();
        boolean removed = this.items.removeIf(item -> item.getProductId().equals(productId));
        if (!removed) {
            throw new OrderBusinessException("Product not found in order");
        }
    }

    public void confirm() {
        checkMutability();
        if (this.items.isEmpty()) {
            throw new OrderBusinessException("Cannot confirm an empty order");
        }

        this.status = OrderStatus.CONFIRMED;
        this.registerEvent(new OrderConfirmedEvent(this.getId(), Instant.now()));
    }

    public void cancel() {
        if (this.status == OrderStatus.CANCELLED) return; // Idempotence
        if (this.status == OrderStatus.CONFIRMED) {
            throw new OrderBusinessException("A confirmed order cannot be cancelled directly");
        }

        this.status = OrderStatus.CANCELLED;
        this.registerEvent(new OrderCancelledEvent(this.getId(), Instant.now()));
    }

    private void checkMutability() {
        if (this.status == OrderStatus.CONFIRMED) {
            throw new OrderBusinessException("A confirmed order cannot be modified");
        }
        if (this.status == OrderStatus.CANCELLED) {
            throw new OrderBusinessException("A cancelled order is final and cannot be modified");
        }
    }

    // Getters pour la lecture (Toujours retourner des collections non mutables)
    public List<OrderItem> getItems() {
        return Collections.unmodifiableList(items);
    }

    public OrderStatus getStatus() {
        return status;
    }

    public static Order restore(OrderId id, OrderStatus status, List<OrderItem> items, Discount discount, Long version) {
        Order order = new Order(id);
        order.status = status;
        order.items.addAll(items);
        order.discount = discount; // On restaure l'état
        order.version = version;
        return order;
    }

    public void ship(String trackingNumber){
        if (this.status != OrderStatus.CONFIRMED) {
            throw new OrderBusinessException("Order must be confirmed before shipped");
        }
        this.status = OrderStatus.SHIPPED;
        this.registerEvent(new OrderShippedEvent(this.getId(), trackingNumber, Instant.now()));
    }

    public void applyDiscount(Discount discount){
        // 🌟 NOUVEAU : On utilise checkMutability pour rejeter si ce n'est pas PENDING
        checkMutability();

        // 🌟 NOUVEAU : On sauvegarde la réduction dans l'objet !
        this.discount = discount;

        this.registerEvent(new OrderDiscountedEvent(this.getId(), discount.codePromo(), discount.percentage(), Instant.now()));
    }

    public @NonNull Money getTotalAmount() {
        // 1. Calcul classique des items
        Money baseTotal = this.getItems().stream()
                .map(OrderItem::calculateTotal)
                .reduce(new Money(BigDecimal.ZERO, "EUR"), Money::add);

        // 🌟 NOUVEAU : 2. On applique la réduction si elle est présente
        if (this.discount != null) {
            return baseTotal.applyPercentage(this.discount);
        }

        return baseTotal;
    }
}