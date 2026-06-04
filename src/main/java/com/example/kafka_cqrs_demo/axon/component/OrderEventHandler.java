package com.example.kafka_cqrs_demo.axon.component;

import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.EventHandler;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderEventHandler {
    @EventHandler
    public void on(OrderCreatedEvent event) {
        log.info("[EventHandler] 成功監聽事件，訂單ID: " + event.getOrderId());
    }
}
