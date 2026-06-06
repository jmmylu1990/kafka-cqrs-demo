package com.example.kafka_cqrs_demo;

import com.example.kafka_cqrs_demo.axon.aggregate.AxonSagaOrderAggregate;
import com.example.kafka_cqrs_demo.axon.command.*;
import com.example.kafka_cqrs_demo.axon.event.*;
import org.axonframework.test.aggregate.AggregateTestFixture;
import org.axonframework.test.aggregate.FixtureConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 訂單聚合根單元測試 (Order Aggregate Unit Test)
 * <p>
 * 本類別採用 Axon 框架提供的 {@link AggregateTestFixture} 工具，對訂單聚合根進行事件溯源單元測試。
 * 測試遵循 Given-When-Then 模式：
 * <ul>
 *   <li>Given：設定該聚合根在過去已經套用並持久化的歷史事件流。</li>
 *   <li>When：發送一個待測試的業務指令。</li>
 *   <li>Then：驗證聚合根執行後是否套用了預期的領域事件，或是拋出了預期的異常。</li>
 * </ul>
 * </p>
 */
public class OrderAggregateTest {

    /** Axon 提供的聚合根測試治具實例 */
    private FixtureConfiguration<AxonSagaOrderAggregate> fixture;

    /** 測試用的訂單唯一識別碼 */
    private final String orderId = "order-123";

    /**
     * 在每個測試案例執行前，初始化測試治具。
     */
    @BeforeEach
    void setUp() {
        fixture = new AggregateTestFixture<>(AxonSagaOrderAggregate.class);
    }

    /**
     * 測試：在全新且無任何歷史活動的狀態下，發送建立訂單指令。
     * 預期：聚合根應順利執行並發布對應的 OrderCreatedEvent。
     */
    @Test
    void testCreateOrder() {
        fixture.givenNoPriorActivity()
            .when(new AxonSagaCreateOrderCommand(orderId, "PRODUCT-001", 1, 1000))
            .expectEvents(new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000));
    }

    /**
     * 測試：在訂單已建立且庫存已確認預留（即狀態為 PENDING_PAYMENT）的前提下，發送付款確認指令。
     * 預期：訂單應順利完成付款，並發布 OrderPaidEvent。
     */
    @Test
    void testPayOrderSuccess() {
        fixture.given(
                new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000),
                new OrderStockReservedEvent(orderId)
            )
            .when(new ConfirmPaymentCommand(orderId))
            .expectEvents(new OrderPaidEvent(orderId, "已付款"));
    }

    /**
     * 測試：在訂單僅處於剛建立 (CREATED) 但尚未確認扣減庫存的狀態下，直接發送付款確認指令。
     * 預期：由於不符合狀態機流程，聚合根應拋出 IllegalStateException 異常拒絕付款。
     */
    @Test
    void testPayOrderInCreatedStatusShouldFail() {
        fixture.given(new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000))
            .when(new ConfirmPaymentCommand(orderId))
            .expectException(IllegalStateException.class);
    }

    /**
     * 測試：在訂單已建立的前提下，發送庫存確認預留指令。
     * 預期：聚合根應順利更新狀態，並發布 OrderStockReservedEvent。
     */
    @Test
    void testConfirmStockReserved() {
        fixture.given(new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000))
            .when(new ConfirmStockReservedCommand(orderId))
            .expectEvents(new OrderStockReservedEvent(orderId));
    }

    /**
     * 測試：在訂單剛建立的前提下，發送取消訂單指令。
     * 預期：訂單應順利取消，並發布 OrderCancelledEvent。
     */
    @Test
    void testCancelOrderSuccess() {
        fixture.given(new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000))
            .when(new CancelOrderCommand(orderId, "不想要了"))
            .expectEvents(new OrderCancelledEvent(orderId, "不想要了"));
    }

    /**
     * 測試：在訂單已經被取消的前提下，嘗試對其發送付款指令。
     * 預期：此為非法的狀態轉移，聚合根應拒絕執行並拋出 IllegalStateException 異常。
     */
    @Test
    void testIllegalStateTransition() {
        fixture.given(
                new OrderCreatedEvent(orderId, "PRODUCT-001", 1, 1000),
                new OrderCancelledEvent(orderId, "取消原因")
            )
            .when(new ConfirmPaymentCommand(orderId))
            .expectException(IllegalStateException.class);
    }
}