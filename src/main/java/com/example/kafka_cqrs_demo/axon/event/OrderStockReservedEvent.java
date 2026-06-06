package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 訂單庫存預留確認事件 (Order Stock Reserved Event)
 * <p>
 * 當訂單聚合根確認收到 Saga 的庫存預留成功指令後，會套用並發布此領域事件。
 * 它會將訂單聚合狀態與 Redis 讀取模型狀態變更為 PENDING_PAYMENT。
 * </p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderStockReservedEvent {

    /** 預留成功之訂單的唯一識別碼 */
    private String orderId;
}
