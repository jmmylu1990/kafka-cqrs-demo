package com.example.kafka_cqrs_demo.axon.saga;

import com.example.kafka_cqrs_demo.axon.command.CancelOrderCommand;
import com.example.kafka_cqrs_demo.axon.command.ConfirmPaymentCommand;
import com.example.kafka_cqrs_demo.axon.command.ConfirmStockReservedCommand;
import com.example.kafka_cqrs_demo.axon.command.ReserveStockCommand;
import com.example.kafka_cqrs_demo.axon.command.DebitWalletCommand;
import com.example.kafka_cqrs_demo.axon.event.OrderCancelledEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderPaidEvent;
import com.example.kafka_cqrs_demo.axon.event.StockFailedEvent;
import com.example.kafka_cqrs_demo.axon.event.StockReservedEvent;
import com.example.kafka_cqrs_demo.axon.event.PaymentStartedEvent;
import com.example.kafka_cqrs_demo.axon.event.WalletDebitedEvent;
import com.example.kafka_cqrs_demo.axon.event.WalletDebitFailedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.annotation.DeadlineHandler;
import org.axonframework.modelling.saga.EndSaga;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.axonframework.spring.stereotype.Saga;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;

/**
 * 訂單協調 Saga 流程管理器 (OrderSaga)
 * <p>
 * 本類別採用基於協調器 (Orchestrator-based) 的 Saga 模式，用以管理跨微服務/聚合的分散式交易。
 * 它會監聽多個領域事件（例如訂單建立、庫存預留成功/失敗、付款成功等），
 * 並負責發送對應的補償或確認指令 (Commands) 來推進業務流程，以達到系統的最終一致性 (Eventual Consistency)。
 * </p>
 * <p>
 * 此外，本 Saga 也引入了 Axon 的 DeadlineManager，用於實作「付款超時自動取消訂單」的逾時補償邏輯。
 * </p>
 */
@Slf4j
@Saga
public class OrderSaga {

    /**
     * 指令閘道器 (Command Gateway)
     * 用於將指令非同步發送給對應的指令處理器 (CommandHandler)。
     * 由於 Saga 會被序列化儲存至 SagaStore，所以此非 transient 的 Spring Bean 依賴欄位必須標註為 transient，
     * 避免在序列化 Saga 實例時發生錯誤。
     */
    @Autowired
    private transient CommandGateway commandGateway;

    /**
     * 逾時管理器 (Deadline Manager)
     * 用於排程並管理特定時間長度後觸發的逾時任務（例如 15 分鐘未付款自動取消）。
     * 同樣需要標註為 transient 以防序列化失敗。
     */
    @Autowired
    private transient DeadlineManager deadlineManager;

    private String userId;

    /**
     * 開始 Saga 流程的事件處理器。
     * 當系統發布 OrderCreatedEvent 時，會自動實例化一個新的 OrderSaga 流程，
     * 並藉由相同的 orderId 進行關聯 (associationProperty = "orderId")。
     * 此步驟會向 Inventory 模組發送 ReserveStockCommand 指令來預扣庫存。
     *
     * @param event 訂單建立事件
     */
    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(OrderCreatedEvent event) {
        log.info("[Saga] 訂單 {}, 開始執行庫存預留", event.getOrderId());
        this.userId = event.getUserId();
        commandGateway.send(new ReserveStockCommand(event.getOrderId(), event.getProductId(), event.getQuantity()));
    }

    /**
     * 庫存預留成功事件處理器。
     * 當監聽到 Inventory 發布的 StockReservedEvent 時，會：
     * 1. 發送 ConfirmStockReservedCommand 給 Order Aggregate 以更新訂單狀態為待付款。
     * 2. 使用 DeadlineManager 排程 15 分鐘的逾時截止時間，若 15 分鐘內未完成付款將觸發 payment-deadline。
     *
     * @param event 庫存預留成功事件
     */
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(StockReservedEvent event) {
        log.info("[Saga] 庫存預留成功，訂單 {} 進入待付款狀態，發送確認確認指令", event.getOrderId());

        // 發送確認指令更新訂單狀態
        commandGateway.send(new ConfirmStockReservedCommand(event.getOrderId()));

        // 排程 15 分鐘的付款截止逾時任務
        deadlineManager.schedule(Duration.ofMinutes(15), "payment-deadline", event.getOrderId());
    }

    /**
     * 付款超時處理器 (Deadline Handler)。
     * 當設定的 15 分鐘截止時間到達且未被手動取消時，Axon 會呼叫此方法。
     *
     * @param orderId 觸發此逾時任務的訂單識別碼
     */
    @DeadlineHandler(deadlineName = "payment-deadline")
    public void onPaymentDeadline(String orderId) {
        log.warn("[Saga] 訂單 {} 付款超時，執行自動取消", orderId);
        commandGateway.send(new CancelOrderCommand(orderId, "付款超時"));
    }

    /**
     * 庫存扣減失敗（庫存不足）的補償事件處理器。
     * 當監聽到 StockFailedEvent 時，代表無法成功預留庫存，
     * 此時需要發送 CancelOrderCommand 指令將訂單狀態改為已取消。
     *
     * @param event 庫存扣減失敗事件
     */
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(StockFailedEvent event) {
        log.info("[Saga] 庫存不足，執行補償取消: {}", event.getOrderId());
        commandGateway.send(new CancelOrderCommand(event.getOrderId(), "庫存不足"));
    }

    /**
     * 付款成功事件處理器。
     * 當使用者順利完成付款並觸發 OrderPaidEvent 時：
     * 1. 說明整個交易流程成功完成，因此標註 @EndSaga 結束此 Saga 實例。
     * 2. 同步手動取消已排程但尚未到期的 payment-deadline 付款超時任務，避免訂單在付款後被誤取消。
     *
     * @param event 訂單已付款成功事件
     */
    @EndSaga
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(OrderPaidEvent event) {
        log.info("[Saga] 訂單 {} 已確認付款，Saga 流程圓滿結束", event.getOrderId());

        // 取消先前設定的付款截止計時器
        deadlineManager.cancelSchedule("payment-deadline", event.getOrderId());
    }

    /**
     * 訂單取消事件處理器。
     * 當監聽到 OrderCancelledEvent 時（不論是手動取消、逾時取消或庫存不足取消）：
     * 1. 標註 @EndSaga 結束此 Saga 實例。
     * 2. 取消先前設定的付款截止計時器（如果存在）。
     *
     * @param event 訂單已取消事件
     */
    @EndSaga
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(OrderCancelledEvent event) {
        log.info("[Saga] 訂單 {} 已取消，Saga 流程結束", event.getOrderId());

        // 取消先前設定的付款截止計時器
        deadlineManager.cancelSchedule("payment-deadline", event.getOrderId());
    }

    /**
     * 處理付款流程開始事件。
     */
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(PaymentStartedEvent event) {
        log.info("[Saga] 訂單 {} 付款流程開始，發送錢包扣款指令，使用者: {}, 金額: {}", 
                event.getOrderId(), event.getUserId(), event.getAmount());
        commandGateway.send(new DebitWalletCommand(event.getUserId(), event.getOrderId(), event.getAmount()));
    }

    /**
     * 處理錢包扣款成功事件。
     */
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(WalletDebitedEvent event) {
        log.info("[Saga] 訂單 {} 錢包扣款成功，發送確認付款指令", event.getOrderId());
        commandGateway.send(new ConfirmPaymentCommand(event.getOrderId()));
    }

    /**
     * 處理錢包扣款失敗事件。
     */
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(WalletDebitFailedEvent event) {
        log.warn("[Saga] 訂單 {} 錢包扣款失敗，原因: {}。開始自動取消訂單", event.getOrderId(), event.getReason());
        commandGateway.send(new CancelOrderCommand(event.getOrderId(), "扣款失敗: " + event.getReason()));
    }
}

