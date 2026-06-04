package com.example.kafka_cqrs_demo.axon.dto;

import lombok.Data;

@Data
public class PayOrderRequest {
    private String orderId;
    private String paymentMethod; // 例如：CREDIT_CARD, LINE_PAY
}
