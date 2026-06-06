package com.example.kafka_cqrs_demo.legacy.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 傳統模式訂單建立事件 (Legacy Order Created Event)
 * <p>
 * 用於在傳統手動 Kafka CQRS 模式下，寫入端將建立好的訂單資訊序列化為事件發送至 Kafka，
 * 並由讀取端消費者消費以同步 Redis 讀取模型。
 * </p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {

    /** 訂單的唯一識別碼 (UUID) */
    private String orderId;

    /** 購買商品的產品唯一識別碼 */
    private String productId;

    /** 購買數量 */
    private int quantity;

    /** 商品單價 */
    private long price;

    /** 事件產生的時間戳記，預設為當前系統時間的毫秒數 */
    private long timestamp = System.currentTimeMillis();

    /**
     * 自訂建構子，不傳入時間戳記時使用。
     *
     * @param orderId 訂單識別碼
     * @param productId 產品識別碼
     * @param quantity 數量
     * @param price 價格
     */
    public OrderCreatedEvent(String orderId, String productId, int quantity, long price) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.price = price;
    }
}
