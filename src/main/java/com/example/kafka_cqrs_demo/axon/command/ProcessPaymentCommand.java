package com.example.kafka_cqrs_demo.axon.command;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * 處理訂單付款指令 (Process Payment Command)
 * <p>
 * 當使用者點擊付款時，控制器發送此指令至訂單聚合根，用以啟動餘額扣除流程。
 * </p>
 */
@Value
public class ProcessPaymentCommand {

    /** 目標訂單聚合識別碼 */
    @TargetAggregateIdentifier
    private String orderId;

    /** 執行扣款的使用者識別碼 */
    private String userId;
}
