package com.example.kafka_cqrs_demo.axon.command;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * 確認庫存預留成功指令 (Confirm Stock Reserved Command)
 * <p>
 * 當庫存預留流程成功時，Saga 會發送此指令通知訂單聚合根，將訂單狀態更新為 PENDING_PAYMENT。
 * </p>
 */
@Value
public class ConfirmStockReservedCommand {

    /**
     * 目標聚合識別碼
     * 指明此庫存確認指令要路由至對應的訂單聚合根實例。
     */
    @TargetAggregateIdentifier
    private String orderId;
}
