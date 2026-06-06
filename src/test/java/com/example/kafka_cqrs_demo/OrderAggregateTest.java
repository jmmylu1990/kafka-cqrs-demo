package com.example.kafka_cqrs_demo;

import com.example.kafka_cqrs_demo.axon.aggregate.AxonSagaOrderAggregate;
import com.example.kafka_cqrs_demo.axon.command.*;
import com.example.kafka_cqrs_demo.axon.event.*;
import org.axonframework.test.aggregate.AggregateTestFixture;
import org.axonframework.test.aggregate.FixtureConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OrderAggregateTest {

    private FixtureConfiguration<AxonSagaOrderAggregate> fixture;
    private final String orderId = "order-123";

    @BeforeEach
    void setUp() {
        fixture = new AggregateTestFixture<>(AxonSagaOrderAggregate.class);
    }

    @Test
    void testCreateOrder() {
        fixture.givenNoPriorActivity()
            .when(new AxonSagaCreateOrderCommand(orderId, "PRODUCT-001", 1, 1000))
            .expectEvents(new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000));
    }

    @Test
    void testPayOrderSuccess() {
        fixture.given(
                new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000),
                new OrderStockReservedEvent(orderId)
            )
            .when(new ConfirmPaymentCommand(orderId))
            .expectEvents(new OrderPaidEvent(orderId, "已付款"));
    }

    @Test
    void testPayOrderInCreatedStatusShouldFail() {
        fixture.given(new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000))
            .when(new ConfirmPaymentCommand(orderId))
            .expectException(IllegalStateException.class);
    }

    @Test
    void testConfirmStockReserved() {
        fixture.given(new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000))
            .when(new ConfirmStockReservedCommand(orderId))
            .expectEvents(new OrderStockReservedEvent(orderId));
    }

    @Test
    void testCancelOrderSuccess() {
        fixture.given(new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000))
            .when(new CancelOrderCommand(orderId, "不想要了"))
            .expectEvents(new OrderCancelledEvent(orderId, "不想要了"));
    }

    @Test
    void testIllegalStateTransition() {
        // 驗證：如果訂單已經取消了，不允許再付款
        fixture.given(
                new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000),
                new OrderCancelledEvent(orderId, "取消原因")
            )
            .when(new ConfirmPaymentCommand(orderId))
            .expectException(IllegalStateException.class); // 預期系統會拋出錯誤
    }
}