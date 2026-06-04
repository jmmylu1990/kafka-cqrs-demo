package com.example.kafka_cqrs_demo.command.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

public @Data class CreateOrderRequest {
    @NotBlank(message = "商品ID不能為空") // 確保字串不為 null 且不為空字串
    private String productId;
    @Min(value = 1, message = "數量至少為 1")
    private int quantity;
    @Min(value = 0, message = "價格不能為負數")
    private long price;
}
