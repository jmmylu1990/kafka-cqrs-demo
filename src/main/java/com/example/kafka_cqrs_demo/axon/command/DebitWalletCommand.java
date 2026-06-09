package com.example.kafka_cqrs_demo.axon.command;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * 扣減錢包餘額指令 (Debit Wallet Command)
 */
@Value
public class DebitWalletCommand {

    /**
     * 目標聚合識別碼
     * 指向要扣款的 WalletAggregate (即 userId)
     */
    @TargetAggregateIdentifier
    private String userId;

    /** 關聯的訂單 ID */
    private String orderId;

    /** 欲扣款的金額 */
    private long amount;
}
