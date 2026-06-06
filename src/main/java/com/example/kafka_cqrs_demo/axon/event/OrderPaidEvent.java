package com.example.kafka_cqrs_demo.axon.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
public @Data class OrderPaidEvent {
    private String orderId;
    private String reason;

    public OrderPaidEvent(String orderId) {
        this.orderId = orderId;
    }
}
