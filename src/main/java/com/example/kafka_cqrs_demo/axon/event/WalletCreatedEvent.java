package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 錢包建立/初始化事件 (Wallet Created Event)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WalletCreatedEvent {

    /** 使用者唯一識別碼 */
    private String userId;

    /** 初始錢包餘額 */
    private long initialBalance;
}
