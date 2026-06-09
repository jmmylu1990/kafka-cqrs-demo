package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 外部庫存預留成功事件 (Stock Reserved Event)
 * <p>
 * 當庫存系統成功扣減並預留訂單所需的商品數量時，由庫存服務發布此事件。
 * </p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StockReservedEvent {

    /** 預留成功之訂單的唯一識別碼 */
    private String orderId;

    /** 庫存已成功鎖定的產品唯一識別碼 */
    private String productId;
}
