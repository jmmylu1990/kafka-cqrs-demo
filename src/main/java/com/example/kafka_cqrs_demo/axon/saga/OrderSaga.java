package com.example.kafka_cqrs_demo.axon.saga;


import com.example.kafka_cqrs_demo.axon.command.CancelOrderCommand;
import com.example.kafka_cqrs_demo.axon.command.ConfirmPaymentCommand;
import com.example.kafka_cqrs_demo.axon.command.ConfirmStockReservedCommand;
import com.example.kafka_cqrs_demo.axon.command.ReserveStockCommand;
import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderPaidEvent;
import com.example.kafka_cqrs_demo.axon.event.StockFailedEvent;
import com.example.kafka_cqrs_demo.axon.event.StockReservedEvent;
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

@Slf4j
@Saga
public class OrderSaga {
    @Autowired
    private transient CommandGateway commandGateway;

    // 注入 Axon 的 Deadline 管理器
    @Autowired
    private transient DeadlineManager deadlineManager;

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(OrderCreatedEvent event) {
        log.info("[Saga] 訂單 {}, 開始執行庫存預留", event.getOrderId());
        commandGateway.send(new ReserveStockCommand(event.getOrderId(), event.getProductId()));
    }

    @SagaEventHandler(associationProperty = "orderId")
    public void handle(StockReservedEvent event) {
        log.info("[Saga] 庫存預留成功，訂單 {} 進入待付款狀態，發送確認確認指令", event.getOrderId());

        // 發送指令給 Order Aggregate 來確認庫存預留已完成，從而改變 Aggregate 的狀態
        commandGateway.send(new ConfirmStockReservedCommand(event.getOrderId()));

        // 合併邏輯：記錄成功後，同時安排 15 分鐘的逾時 Deadline
        deadlineManager.schedule(Duration.ofMinutes(15), "payment-deadline", event.getOrderId());
    }

    @EndSaga //這裡必須標註，表示超時處理完畢，Saga 生命週期結束
    @DeadlineHandler(deadlineName = "payment-deadline")
    public void onPaymentDeadline(String orderId) {
        log.warn("[Saga] 訂單 {} 付款超時，執行自動取消", orderId);
        commandGateway.send(new CancelOrderCommand(orderId, "付款超時"));
    }

    @EndSaga //這裡也補上，表示補償處理完畢，Saga 結束
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(StockFailedEvent event) {
        log.info("[Saga] 庫存不足，執行補償取消: {}", event.getOrderId());
        commandGateway.send(new CancelOrderCommand(event.getOrderId(), "庫存不足"));
    }

    @EndSaga // 付款確認後，Saga 任務圓滿完成
    @SagaEventHandler(associationProperty = "orderId")
    public void handle(OrderPaidEvent event) { // 👈 監聽 OrderPaidEvent 事件
        log.info("[Saga] 訂單 {} 已確認付款，Saga 流程圓滿結束", event.getOrderId());

        // 如果有必要，手動取消還沒到期的付款逾時 Deadline
        deadlineManager.cancelSchedule("payment-deadline", event.getOrderId());
    }
}

