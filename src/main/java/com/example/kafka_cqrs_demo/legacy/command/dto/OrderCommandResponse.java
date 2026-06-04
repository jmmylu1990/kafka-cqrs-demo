package com.example.kafka_cqrs_demo.legacy.command.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
public @Data class OrderCommandResponse {
    private String orderId;
    private String status;  // 例如: "PENDING"
    private String message;
}
