package com.example.hexagonal_completed_design.order.application.service;

import com.example.hexagonal_completed_design.order.adapter.in.rest.dto.response.DiscountedOrderResponse;
import com.example.hexagonal_completed_design.order.application.command.*;
import com.example.hexagonal_completed_design.order.application.port.in.ManageOrderUseCase;
import com.example.hexagonal_completed_design.order.application.port.out.DiscountPort;
import com.example.hexagonal_completed_design.order.application.port.out.EventPublisherPort;
import com.example.hexagonal_completed_design.order.application.port.out.PaymentPort;
import com.example.hexagonal_completed_design.order.application.port.out.ShipPort;
import com.example.hexagonal_completed_design.order.domain.aggregate.Order;
import com.example.hexagonal_completed_design.order.domain.aggregate.OrderItem;
import com.example.hexagonal_completed_design.order.domain.exception.OrderBusinessException;
import com.example.hexagonal_completed_design.order.domain.exception.PromoCodeException;
import com.example.hexagonal_completed_design.order.domain.repository.OrderRepository;
import com.example.hexagonal_completed_design.order.domain.valueobject.*;
import org.jspecify.annotations.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class OrderApplicationService implements ManageOrderUseCase {

    private final OrderRepository orderRepository;
    private final PaymentPort paymentPort;
    private final EventPublisherPort eventPublisherPort;
    private final ShipPort shipPort;
    private final DiscountPort discountPort;

    // Injection de dépendances par constructeur (agnostique du framework)
    public OrderApplicationService(
            OrderRepository orderRepository,
            PaymentPort paymentPort,
            EventPublisherPort eventPublisherPort, ShipPort shipPort, DiscountPort discountPort) {
        this.orderRepository = orderRepository;
        this.paymentPort = paymentPort;
        this.eventPublisherPort = eventPublisherPort;
        this.shipPort = shipPort;
        this.discountPort = discountPort;
    }

    @Override
    public void createOrder(CreateOrderCommand command) {
        // 1. Instanciation métier
        OrderId orderId = new OrderId(command.orderId());
        Order order = Order.create(orderId);

        // 2. Persistance et publication
        saveAndPublish(order);
    }

    @Override
    public void addOrderItem(AddOrderItemCommand command) {
        // 1. Récupération
        Order order = getOrder(new OrderId(command.orderId()));

        // 2. Action métier (C'est l'Aggregate qui valide les règles, pas le service)
        order.addItem(
                new ProductId(command.productId()),
                command.quantity(),
                new Money(command.amount(), command.currency())
        );

        // 3. Persistance et publication
        saveAndPublish(order);
    }

    @Override
    public void confirmOrder(ConfirmOrderCommand command) {
        Order order = getOrder(new OrderId(command.orderId()));

        Money totalAmount = order.getTotalAmount();
        // Orchestration d'un service externe (Paiement)
        boolean paymentSuccessful = paymentPort.processPayment(order.getId(), totalAmount);

        if (!paymentSuccessful) {
            throw new RuntimeException("Payment processing failed for Order: " + order.getId().value());
        }

        // Action métier
        order.confirm();

        // Persistance et publication des Domain Events (OrderConfirmedEvent sera envoyé ici)
        saveAndPublish(order);
    }

    @Override
    public void cancelOrder(CancelOrderCommand command) {
        Order order = getOrder(new OrderId(command.orderId()));
        order.cancel(); // L'Aggregate s'occupe de la règle métier et génère l'événement
        saveAndPublish(order);
    }

    @Override
    public void shipOrder(ShipOrderCommand command) {
        Order order = getOrder(new OrderId(command.orderId()));
        String TrackingNumber = this.shipPort.getShippingNumber(order.getId());
        order.ship(TrackingNumber);
        saveAndPublish(order);
    }

    @Override
    public void applyDiscount(DiscountCommand command, String codePromo) {
        // 1. Récupérer la commande
        Order order = getOrder(new OrderId(command.orderId()));

        // 2. Demander au port externe la valeur de la promo (ex: 10)
        BigDecimal discountPercentage = this.discountPort.getDiscount(codePromo);

        // 3. Créer le concept métier "Discount"
        Discount discount = new Discount(discountPercentage, codePromo);

        // 4. Donner l'ordre au Domaine (qui vérifiera le statut tout seul)
        order.applyDiscount(discount);

        // 5. Sauvegarder
        saveAndPublish(order);
    }

    public void checkDiscountCode(String codePromo){
        if(!this.discountPort.checkPromoCode(codePromo)){
            throw new PromoCodeException("Code Promo not found " + codePromo);
        }
    }

    private Order getOrder(OrderId id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new OrderBusinessException("Order not found with id: " + id.value()));
    }

    private void saveAndPublish(Order order) {
        // Sauvegarde de l'état
        orderRepository.save(order);

        // Dépilement et publication de tous les événements accumulés dans l'Aggregate
        order.getDomainEvents().forEach(eventPublisherPort::publish);
        order.clearDomainEvents();
    }
}