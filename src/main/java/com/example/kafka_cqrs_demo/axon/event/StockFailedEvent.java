package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 庫存預留失敗事件 (Stock Failed Event)
 * <p>
 * 當庫存系統在預扣訂單商品的庫存時，因庫存不足或其他原因失敗時發布此事件。
 * </p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StockFailedEvent {

    /** 預留失敗之訂單的唯一識別碼 */
    private String orderId;

    /** 庫存預留失敗的詳細原因描述 */
    private String reason;
}
