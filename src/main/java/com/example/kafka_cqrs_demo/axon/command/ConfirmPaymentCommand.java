package com.example.kafka_cqrs_demo.axon.command;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * 確認付款指令 (Confirm Payment Command)
 * <p>
 * 當外部支付系統或測試 API 呼叫發起訂單付款成功時，發送此指令將訂單狀態更新為 PAID。
 * </p>
 */
@Value
public class ConfirmPaymentCommand {

    /**
     * 目標聚合識別碼
     * 指明此付款確認指令要路由到的訂單聚合根實例。
     */
    @TargetAggregateIdentifier
    private String orderId;
}
