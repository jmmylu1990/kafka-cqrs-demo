package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 錢包退款成功事件 (Wallet Refunded Event)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WalletRefundedEvent {

    /** 使用者 ID */
    private String userId;

    /** 關聯的訂單 ID */
    private String orderId;

    /** 退款的金額 */
    private long amount;
}
