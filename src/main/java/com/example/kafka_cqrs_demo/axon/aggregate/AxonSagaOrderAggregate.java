package com.example.kafka_cqrs_demo.axon.aggregate;

import com.example.kafka_cqrs_demo.axon.command.AxonSagaCreateOrderCommand;
import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.spring.stereotype.Aggregate;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

/**
 * 訂單聚合根 (Order Aggregate)
 * 聚合根是領域模型的邊界，所有關於「訂單」的業務變更都必須透過它來進行。
 * 這裡確保了資料的一致性與業務邏輯的封裝。
 */
@Slf4j
@Aggregate
public class AxonSagaOrderAggregate {

    /**
     * @AggregateIdentifier 是聚合根的唯一識別 ID。
     * 它必須對應到 Command 中的 @TargetAggregateIdentifier。
     * Axon 透過此 ID 將指令 (Command) 路由到正確的物件實體。
     */
    @AggregateIdentifier
    private String orderId;
    private String productId;
    private int quantity;
    private long price;

    /**
     * 無參建構子 (No-argument Constructor)
     * 這是 Axon 的技術需求。當從 Event Store 恢復 Aggregate 狀態時，
     * Axon 會利用反射機制呼叫此建構子建立空物件，再逐一重放 (Replay) 事件。
     */
    protected AxonSagaOrderAggregate() {
    }

    /**
     * 指令處理器 (Command Handler)
     * 當 CommandGateway 發送 CreateOrderCommand 時，Axon 會找到此處進行處理。
     * * 職責：
     * 1. 執行商業規則檢查 (如：價格是否 > 0，庫存是否足夠)。
     * 2. 若規則通過，執行 apply() 方法將變更「發布」為事件。
     */
    @CommandHandler
    public AxonSagaOrderAggregate(AxonSagaCreateOrderCommand command) {
        log.info("DEBUG: Aggregate 接收到指令: {}" ,command.getClass().getName());
        // [商業驗證區]
        if (command.getPrice() < 0) {
            throw new IllegalArgumentException("價格不能為負數");
        }

        // [發布事件]
        // apply() 會做兩件事：
        // 1. 將事件存入 Event Store (持久化)。
        // 2. 觸發下方的 @EventSourcingHandler 來更新本物件狀態。
        apply(new OrderCreatedEvent(
            command.getOrderId(),
            command.getProductId(),
            command.getQuantity(),
            command.getPrice()
        ));
    }

    /**
     * 事件來源處理器 (Event Sourcing Handler)
     * 這是「更新自身狀態」的地方。
     * * 職責：
     * 當事件發生後，Axon 會呼叫此方法來更新物件的欄位。
     * 注意：此方法「絕對不能」包含業務邏輯驗證，它只負責「賦值」。
     * 因為在系統重啟時，Axon 會重放所有歷史事件，每次重放都會呼叫此方法。
     */
    @EventSourcingHandler
    public void on(OrderCreatedEvent event) {
        log.info("[EventSourcingHandler] 狀態已更新: {}" ,event.getOrderId());
        this.orderId = event.getOrderId();
        this.productId = event.getProductId();
        this.quantity = event.getQuantity();
        this.price = event.getPrice();
    }
}