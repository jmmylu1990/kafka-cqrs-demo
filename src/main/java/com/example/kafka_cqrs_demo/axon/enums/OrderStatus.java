package com.example.kafka_cqrs_demo.axon.enums;

public enum OrderStatus {
    CREATED,        // 訂單已建立（等待付款）
    PAID,           // 已付款（庫存已鎖定）
    SHIPPED,        // 已出貨
    DELIVERED,      // 已送達
    CANCELLED,      // 已取消
    REFUNDED        // 已退款
}
