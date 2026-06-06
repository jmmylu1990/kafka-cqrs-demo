package com.example.kafka_cqrs_demo.axon.event;

import lombok.Value;

@Value
public class StockReservedEvent {
    private String orderId;
    private String productId;
}
