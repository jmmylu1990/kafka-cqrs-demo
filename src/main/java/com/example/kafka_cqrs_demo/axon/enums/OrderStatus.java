package com.example.kafka_cqrs_demo.axon.enums;

/**
 * 訂單狀態列舉 (Order Status Enum)
 * <p>
 * 定義訂單生命週期中所有可能的階段狀態，主要用於訂單聚合根的狀態轉移判定與讀取模型的展示。
 * </p>
 */
public enum OrderStatus {

    /** 訂單已建立，此時正等待 Saga 發送庫存預留確認 */
    CREATED,

    /** 庫存預扣成功，訂單正處於待付款狀態 */
    PENDING_PAYMENT,

    /** 訂單付款確認成功，Saga 圓滿完成 */
    PAID,

    /** 商品已打包出貨中，此狀態下無法取消訂單 */
    SHIPPED,

    /** 商品已送達目的地，此狀態下交易完成 */
    DELIVERED,

    /** 訂單已取消（可能由於付款超時、庫存不足或手動取消） */
    CANCELLED,

    /** 訂單退款完成 */
    REFUNDED
}
