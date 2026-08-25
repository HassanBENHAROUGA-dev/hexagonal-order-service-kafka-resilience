package com.example.hexagonal_completed_design.order.adapter.out.persistance.mapper;

import com.example.hexagonal_completed_design.order.adapter.out.persistance.entity.OrderEntity;
import com.example.hexagonal_completed_design.order.adapter.out.persistance.entity.OrderItemEntity;
import com.example.hexagonal_completed_design.order.domain.aggregate.Order;
import com.example.hexagonal_completed_design.order.domain.aggregate.OrderItem;
import com.example.hexagonal_completed_design.order.domain.valueobject.Discount;
import com.example.hexagonal_completed_design.order.domain.valueobject.Money;
import com.example.hexagonal_completed_design.order.domain.valueobject.OrderId;
import com.example.hexagonal_completed_design.order.domain.valueobject.ProductId;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class OrderMapper {

    public static OrderEntity toEntity(Order order) {
        List<OrderItemEntity> itemEntities = order.getItems().stream()
                .map(item -> new OrderItemEntity(
                        item.getProductId().value(),
                        item.getQuantity(),
                        item.getUnitPrice().amount(),
                        item.getUnitPrice().currency()
                ))
                .collect(Collectors.toList());

        // 1. On extrait les valeurs du Discount s'il existe
        String discountCode = null;
        BigDecimal discountPercentage = null;

        if (order.getDiscount() != null) {
            discountCode = order.getDiscount().codePromo();
            discountPercentage = order.getDiscount().percentage();
        }

        // 2. On passe les nouvelles valeurs au constructeur de l'Entité
        OrderEntity entity = new OrderEntity(
                order.getId().value(),
                order.getStatus(),
                itemEntities,
                discountCode,
                discountPercentage
        );

        // 🌟 NOUVEAU : On donne la version à JPA
        entity.setVersion(order.getVersion());

        return entity;
    }

    public static Order toDomain(OrderEntity entity) {
        List<OrderItem> domainItems = entity.getItems().stream()
                .map(itemEntity -> OrderItem.restore(
                        new ProductId(itemEntity.getProductId()),
                        itemEntity.getQuantity(),
                        new Money(itemEntity.getUnitPrice(), itemEntity.getCurrency())
                ))
                .collect(Collectors.toList());

        // 3. On recrée l'objet Value Object Discount si la base contient les infos
        Discount discount = null;
        if (entity.getDiscountCode() != null && entity.getDiscountPercentage() != null) {
            // Attention à l'ordre des paramètres selon comment tu as défini ton record Discount
            discount = new Discount(entity.getDiscountPercentage(), entity.getDiscountCode());
        }

        // 4. On restaure le Domaine avec le discount
        return Order.restore(
                new OrderId(entity.getId()),
                entity.getStatus(),
                domainItems,
                discount,
                entity.getVersion()
        );
    }
}
