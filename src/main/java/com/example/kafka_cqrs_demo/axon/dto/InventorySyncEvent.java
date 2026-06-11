package com.example.kafka_cqrs_demo.axon.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 庫存非同步同步事件 (Inventory Sync Event)
 * <p>
 * 用於在 Redis 庫存扣減或釋放成功後，作為 Kafka 訊息發送，
 * 由背景消費者監聽並異步寫回 MySQL 資料庫，以達到最終一致性。
 * </p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class InventorySyncEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 訂單唯一識別碼 */
    private String orderId;

    /** 產品識別碼 */
    private String productId;

    /** 操作數量 */
    private int quantity;

    /** 同步動作類別：RESERVE (預留), COMMIT (完成交易), RELEASE (釋放), REFUND (已付款退款) */
    private String actionType;

    /** 事件觸發時間戳記 */
    private long timestamp;
}
