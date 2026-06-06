package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 訂單已付款事件 (Order Paid Event)
 * <p>
 * 當訂單聚合根確認接收到付款並通過驗證後，會套用並發布此領域事件。
 * 此事件將會用來結束對應的 Saga 生命週期流程，並通知 Projection 將讀取模型更新為已付款。
 * </p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderPaidEvent {

    /** 已完成付款之訂單的唯一識別碼 */
    private String orderId;

    /** 付款備註或取消原因（此處主要作為狀態變更原因說明） */
    private String reason;

    /**
     * 單參數建構子，用以在無須提供額外說明時建立事件實例。
     *
     * @param orderId 訂單唯一識別碼
     */
    public OrderPaidEvent(String orderId) {
        this.orderId = orderId;
    }
}
