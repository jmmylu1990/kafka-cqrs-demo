package com.example.kafka_cqrs_demo.axon.dto;

import lombok.Data;

@Data
public class CancelOrderRequest {
    private String orderId;
    private String reason; // 例如：顧客後悔、商品缺貨
}