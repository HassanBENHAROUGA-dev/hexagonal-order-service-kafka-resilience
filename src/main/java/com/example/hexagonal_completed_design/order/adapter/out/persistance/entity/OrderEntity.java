package com.example.hexagonal_completed_design.order.adapter.out.persistance.entity;

import com.example.hexagonal_completed_design.order.domain.valueobject.OrderStatus;
import jakarta.persistence.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    private UUID id;

    @Version
    private Long version;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    // --- NOUVEAUX CHAMPS POUR LE DISCOUNT ---
    @Column(name = "discount_code")
    private String discountCode;

    @Column(name = "discount_percentage")
    private BigDecimal discountPercentage;
    // ----------------------------------------

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    @JoinColumn(name = "order_id")
    private List<OrderItemEntity> items = new ArrayList<>();

    protected OrderEntity() {}

    public OrderEntity(UUID id, OrderStatus status, List<OrderItemEntity> items, String discountCode, BigDecimal discountPercentage) {
        this.id = id;
        this.status = status;
        this.items = items;
        this.discountCode = discountCode;
        this.discountPercentage = discountPercentage;
    }

    public String getDiscountCode() { return discountCode; }
    public void setDiscountCode(String discountCode) { this.discountCode = discountCode; }

    public BigDecimal getDiscountPercentage() { return discountPercentage; }
    public void setDiscountPercentage(BigDecimal discountPercentage) { this.discountPercentage = discountPercentage; }
    // Getters et Setters simples (JPA standard)
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    public OrderStatus getStatus() { return status; }
    public void setStatus(OrderStatus status) { this.status = status; }
    public List<OrderItemEntity> getItems() { return items; }
    public void setItems(List<OrderItemEntity> items) { this.items = items; }

    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }
}
