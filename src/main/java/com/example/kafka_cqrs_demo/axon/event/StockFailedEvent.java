package com.example.kafka_cqrs_demo.axon.event;

import lombok.Value;

@Value
public class StockFailedEvent {
    private String orderId;
    private String reason;
}
