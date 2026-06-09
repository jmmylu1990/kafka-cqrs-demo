package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 訂單付款流程開始事件 (Payment Started Event)
 * <p>
 * 當訂單聚合根接受 {@link com.example.kafka_cqrs_demo.axon.command.ProcessPaymentCommand} 後發布。
 * Saga 接收此事件後會向錢包服務發送扣款指令。
 * </p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentStartedEvent {

    /** 訂單 ID */
    private String orderId;

    /** 使用者 ID */
    private String userId;

    /** 扣款總金額 (price * quantity) */
    private long amount;
}
