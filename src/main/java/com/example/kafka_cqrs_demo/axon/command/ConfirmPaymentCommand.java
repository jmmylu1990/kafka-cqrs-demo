package com.example.kafka_cqrs_demo.axon.command;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;
@Value
public class ConfirmPaymentCommand {
    @TargetAggregateIdentifier
    private String orderId;
}
