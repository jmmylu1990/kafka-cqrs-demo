package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 訂單已取消事件 (Order Cancelled Event)
 * <p>
 * 當訂單聚合根成功執行取消操作後，會套用並發布此領域事件。
 * 系統其他組件（如 Saga 或 Projection 投影器）將根據此事件進行對應的狀態清理或退款處理。
 * </p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderCancelledEvent {

    /** 被取消訂單的唯一識別碼 */
    private String orderId;

    /** 訂單取消的詳細原因描述 */
    private String reason;
}
