package com.example.hexagonal_completed_design.unit;

import com.example.hexagonal_completed_design.order.application.command.DiscountCommand;
import com.example.hexagonal_completed_design.order.domain.aggregate.Order;
import com.example.hexagonal_completed_design.order.domain.exception.OrderBusinessException;
import com.example.hexagonal_completed_design.order.domain.valueobject.Discount;
import com.example.hexagonal_completed_design.order.domain.valueobject.Money;
import com.example.hexagonal_completed_design.order.domain.valueobject.OrderId;
import com.example.hexagonal_completed_design.order.domain.valueobject.ProductId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class OrderTest {


    @Test
    void should_calculate_total_without_discount() {
        // GIVEN
        Order order = Order.create(new OrderId(UUID.randomUUID()));
        order.addItem(new ProductId(UUID.randomUUID()), 1, new Money(BigDecimal.valueOf(20), "EUR"));
        order.addItem(new ProductId(UUID.randomUUID()), 1, new Money(BigDecimal.valueOf(30), "EUR"));

        // WHEN
        Money result = order.getTotalAmount();

        // THEN
        assertEquals(BigDecimal.valueOf(50), result.amount());
        assertNull(order.getDiscount());
        System.out.println(result.amount() + " and " + BigDecimal.valueOf(50));
    }

    @Test
    void should_calculate_total_with_discount() {
        // GIVEN
        Order order = Order.create(new OrderId(UUID.randomUUID()));
        order.addItem(new ProductId(UUID.randomUUID()), 1, new Money(BigDecimal.valueOf(100), "EUR"));

        DiscountCommand discountCommand = new DiscountCommand(order.getId().value());
        // WHEN
        Discount discount = new Discount(BigDecimal.valueOf(20), "SUMMER20");
        order.applyDiscount(discount);
        Money result = order.getTotalAmount();

        // THEN
        assertEquals(0, BigDecimal.valueOf(80).compareTo(result.amount()));
        assertNotNull(order.getDiscount());
    }

    @Test
    void should_throw_exception_when_applying_discount_on_confirmed_order() {
        // GIVEN
        Order order = Order.create(new OrderId(UUID.randomUUID()));
        order.addItem(new ProductId(UUID.randomUUID()), 1, new Money(BigDecimal.valueOf(100), "EUR"));
        order.confirm(); // La commande est confirmée !

        // WHEN & THEN
        Discount discount = new Discount(BigDecimal.valueOf(20), "SUMMER20");
        //order.applyDiscount(discount);

        OrderBusinessException exception = assertThrows(
                OrderBusinessException.class,
                () -> order.applyDiscount(discount)
        );

        assertEquals("A confirmed order cannot be modified", exception.getMessage());
    }
}
