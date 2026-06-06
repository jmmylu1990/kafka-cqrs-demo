package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.command.ReserveStockCommand;
import com.example.kafka_cqrs_demo.axon.event.StockFailedEvent;
import com.example.kafka_cqrs_demo.axon.event.StockReservedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class InventoryService {
    private final EventGateway eventGateway;

    public InventoryService(EventGateway eventGateway) {
        this.eventGateway = eventGateway;
    }

    @CommandHandler
    public void handle(ReserveStockCommand command) {
        log.info("[InventoryService] 收到庫存預留請求: {}", command.getOrderId());

        // 模擬業務邏輯：如果 productId 是 "OUT_OF_STOCK" 就模擬失敗
        if ("OUT_OF_STOCK".equals(command.getProductId())) {
            log.warn("[InventoryService] 庫存不足!");
            eventGateway.publish(new StockFailedEvent(command.getOrderId(), "庫存不足"));
        } else {
            log.info("[InventoryService] 庫存扣除成功");
            eventGateway.publish(new StockReservedEvent(command.getOrderId(), command.getProductId()));
        }
    }
}
