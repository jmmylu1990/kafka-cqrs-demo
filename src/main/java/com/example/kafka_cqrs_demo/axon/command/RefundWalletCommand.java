package com.example.kafka_cqrs_demo.axon.command;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * 退款至錢包餘額指令 (Refund Wallet Command)
 */
@Value
public class RefundWalletCommand {

    /**
     * 目標聚合識別碼
     * 指向要退款的 WalletAggregate (即 userId)
     */
    @TargetAggregateIdentifier
    private String userId;

    /** 關聯的訂單 ID */
    private String orderId;

    /** 欲退款的金額 */
    private long amount;
}
