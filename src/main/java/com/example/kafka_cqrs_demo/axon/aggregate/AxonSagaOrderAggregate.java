package com.example.kafka_cqrs_demo.axon.aggregate;

import com.example.kafka_cqrs_demo.axon.command.AxonSagaCreateOrderCommand;
import com.example.kafka_cqrs_demo.axon.command.CancelOrderCommand;
import com.example.kafka_cqrs_demo.axon.command.ConfirmPaymentCommand;
import com.example.kafka_cqrs_demo.axon.command.ConfirmStockReservedCommand;
import com.example.kafka_cqrs_demo.axon.enums.OrderStatus;
import com.example.kafka_cqrs_demo.axon.event.OrderCancelledEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderPaidEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderStockReservedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.spring.stereotype.Aggregate;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

/**
 * 訂單聚合根 (Order Aggregate)
 * 聚合根是領域驅動設計 (DDD) 的邊界，所有關於「訂單」的業務變更都必須透過它來進行。
 * 它確保了訂單資料的一致性、業務規則的執行，以及狀態變更的完整性。
 */
@Slf4j
@Aggregate
public class AxonSagaOrderAggregate {

    /** 聚合根 ID，作為事件儲存的唯一索引 */
    @AggregateIdentifier
    private String orderId;
    private String productId;
    private int quantity;
    private long price;

    private String reason;

    /** 訂單狀態：管理訂單生命週期的核心欄位 */
    private OrderStatus status;

    /**
     * 無參建構子：Axon 的技術需求。
     * 當從 Event Store 恢復 Aggregate 狀態時，Axon 會呼叫此建構子建立空物件，
     * 並透過 EventSourcingHandler 重放事件來恢復內部欄位的值。
     */
    protected AxonSagaOrderAggregate() {}

    /**
     * 訂單建立指令處理器 (Command Handler)
     * 處理建立訂單的意圖。在建立前執行商業規則檢查，確認資料合法性。
     */
    @CommandHandler
    public AxonSagaOrderAggregate(AxonSagaCreateOrderCommand command) {
        log.info("DEBUG: Aggregate 接收到建立指令: {}", command.getOrderId());
        if (command.getPrice() < 0) {
            throw new IllegalArgumentException("價格不能為負數");
        }

        // 發布事件：此動作會持久化至 Event Store，並觸發下方的 on() 事件處理器
        apply(new OrderCreatedEvent(
            command.getOrderId(),
            command.getProductId(),
            command.getQuantity(),
            command.getPrice()
        ));
    }

    /**
     * 付款指令處理器
     * [狀態機邏輯]：
     * 1. 為了確保業務流程的順序性，訂單必須先經過「庫存預扣」階段才能付款。
     * 2. 允許從 PENDING_PAYMENT (已成功鎖定庫存) 或特殊情況下的 CREATED (若無庫存機制) 進行付款。
     * 3. 若狀態不符，說明流程異常或重複付款，拋出異常以拒絕指令。
     */
    @CommandHandler
    public void handle(ConfirmPaymentCommand command) {
        log.info("處理付款指令: {}", command.getOrderId());

        // 防禦性檢查：防止非預期的狀態變更
        boolean isEligibleForPayment = (this.status == OrderStatus.PENDING_PAYMENT);

        if (!isEligibleForPayment) {
            log.error("付款失敗：訂單 {} 當前狀態 {} 不允許執行付款", this.orderId, this.status);
            throw new IllegalStateException("訂單目前狀態不允許付款: " + this.status);
        }

        // 業務規則通過，發布付款成功事件
        apply(new OrderPaidEvent(this.orderId, "已付款"));
    }

    /**
     * 取消指令處理器
     * [狀態機邏輯]：透過檢查當前狀態，防止訂單在出貨或送達後被錯誤取消。
     */
    @CommandHandler
    public void handle(CancelOrderCommand command) {
        if (this.status == OrderStatus.SHIPPED || this.status == OrderStatus.DELIVERED) {
            throw new IllegalStateException("已出貨或送達的訂單無法取消");
        }
        // 將 command 中的 reason 傳給事件
        apply(new OrderCancelledEvent(command.getOrderId(), command.getReason()));
    }

    @CommandHandler
    public void handle(ConfirmStockReservedCommand command) {
        log.info("處理確認庫存保留指令: {}", command.getOrderId());
        apply(new OrderStockReservedEvent(command.getOrderId()));
    }

    // --- 事件來源處理器 (Event Sourcing Handlers) ---
    // 這些方法「不包含」業務邏輯，僅負責「狀態賦值」。
    // 它們是事件溯源 (Event Sourcing) 的靈魂，確保資料恢復時能回到正確的狀態。

    @EventSourcingHandler
    public void on(OrderCreatedEvent event) {
        log.info("[EventSourcingHandler] 訂單建立，初始化狀態: {}", event.getOrderId());

        // 將所有需要的屬性進行初始化
        this.orderId = event.getOrderId();
        this.productId = event.getProductId();
        this.quantity = event.getQuantity();
        this.price = event.getPrice();

        // 設定狀態
        this.status = OrderStatus.CREATED;
    }

    @EventSourcingHandler
    public void on(OrderPaidEvent event) {
        log.info("[EventSourcingHandler] 訂單已付款: {}", this.orderId);
        this.status = OrderStatus.PAID;
    }

    @EventSourcingHandler
    public void on(OrderCancelledEvent event) {
        this.status = OrderStatus.CANCELLED;
        log.info("[EventSourcingHandler] 訂單已取消，原因: {}", event.getReason());
    }

    @EventSourcingHandler
    public void on(OrderStockReservedEvent event) {
        // [狀態變更]：當 Saga 確保庫存預留成功，Aggregate 同步更新狀態
        this.status = OrderStatus.PENDING_PAYMENT;
        log.info("[EventSourcingHandler] 庫存預留成功，訂單狀態變更為 PENDING_PAYMENT");
    }


}