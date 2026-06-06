package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 訂單建立事件 (Order Created Event)
 * <p>
 * 當訂單聚合根建立時，會套用並持久化此領域事件。
 * 這是訂單生命週期的起點事件，會驅動 Saga 開始執行庫存預留流程，並驅動 Projection 初始化 Redis 讀取模型。
 * </p>
 */
@Data
@NoArgsConstructor // Axon 反序列化需要無參建構子
@AllArgsConstructor
public class OrderCreatedEvent {

    /** 新建立訂單的唯一識別碼 (UUID) */
    private String orderId;

    /** 訂單所購買的產品識別碼 */
    private String productId;

    /** 購買的商品數量 */
    private int quantity;

    /** 商品單價，單位為最小貨幣單位 */
    private long price;
}