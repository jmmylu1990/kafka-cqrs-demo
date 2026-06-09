package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 扣減錢包失敗事件 (Wallet Debit Failed Event)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WalletDebitFailedEvent {

    /** 使用者 ID */
    private String userId;

    /** 關聯的訂單 ID */
    private String orderId;

    /** 欲扣款的金額 */
    private long amount;

    /** 失敗原因 */
    private String reason;
}
