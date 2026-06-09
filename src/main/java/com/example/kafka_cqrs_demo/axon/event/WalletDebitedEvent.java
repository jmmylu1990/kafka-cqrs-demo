package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 扣減錢包成功事件 (Wallet Debited Event)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WalletDebitedEvent {

    /** 使用者 ID */
    private String userId;

    /** 關聯的訂單 ID */
    private String orderId;

    /** 扣減的金額 */
    private long amount;
}
