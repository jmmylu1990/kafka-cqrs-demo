package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor // Axon 序列化必備
@AllArgsConstructor
public class OrderCreatedEvent {
    private String orderId;
    private String productId;
    private int quantity;
    private long price;
}