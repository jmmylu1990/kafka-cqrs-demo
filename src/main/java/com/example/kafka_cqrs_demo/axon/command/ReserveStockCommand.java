package com.example.kafka_cqrs_demo.axon.command;

import org.axonframework.modelling.command.TargetAggregateIdentifier;
import lombok.Value;
@Value
public class ReserveStockCommand {
    @TargetAggregateIdentifier
    private String orderId;    // 用 orderId 來關聯這個預留動作
    private String productId;
}
