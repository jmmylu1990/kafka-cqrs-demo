package com.example.kafka_cqrs_demo.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public @Data class OrderCreatedEvent {
    private String orderId;
    private String productId;
    private int quantity;
    private long price;
    private long timestamp = System.currentTimeMillis();

    public OrderCreatedEvent(String orderId, String productId, int quantity, long price) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.price = price;
    }
}
