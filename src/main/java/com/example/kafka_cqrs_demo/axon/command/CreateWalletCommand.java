package com.example.kafka_cqrs_demo.axon.command;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * 建立/初始化錢包指令 (Create Wallet Command)
 */
@Value
public class CreateWalletCommand {

    /** 使用者唯一識別碼 */
    @TargetAggregateIdentifier
    private String userId;

    /** 初始錢包餘額 */
    private long initialBalance;
}
