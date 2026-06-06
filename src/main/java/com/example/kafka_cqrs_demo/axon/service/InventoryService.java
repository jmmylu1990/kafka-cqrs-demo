package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.command.ReserveStockCommand;
import com.example.kafka_cqrs_demo.axon.event.StockFailedEvent;
import com.example.kafka_cqrs_demo.axon.event.StockReservedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.springframework.stereotype.Service;

/**
 * 模擬庫存服務 (Inventory Service)
 * <p>
 * 在微服務架構中，此類別通常代表庫存服務的指令接口。
 * 它負責處理來自 Saga 流程的庫存扣減/預留請求。
 * 本服務接收到 {@link ReserveStockCommand} 指令後，在內部執行預留逻辑，
 * 並藉由 {@link EventGateway} 發布 StockReservedEvent（成功）或 StockFailedEvent（失敗）領域事件，
 * 以便讓 Saga 協調器推進交易流程。
 * </p>
 */
@Slf4j
@Service
public class InventoryService {

    /** Axon 框架提供的事件發布閘道器，用於向全域廣播領域事件 */
    private final EventGateway eventGateway;

    /**
     * 庫存服務建構子。
     *
     * @param eventGateway 用以發布庫存扣減事件的 EventGateway 實例
     */
    public InventoryService(EventGateway eventGateway) {
        this.eventGateway = eventGateway;
    }

    /**
     * 處理預留庫存指令的 CommandHandler。
     * <p>
     * 業務邏輯模擬：
     * 1. 若商品 ID 等於 "OUT_OF_STOCK"，則模擬庫存扣減失敗，發布 {@link StockFailedEvent}。
     * 2. 若商品 ID 為其他值，則模擬庫存扣減成功，發布 {@link StockReservedEvent}。
     * </p>
     *
     * @param command 包含訂單與商品 ID 的預扣庫存指令
     */
    @CommandHandler
    public void handle(ReserveStockCommand command) {
        log.info("[InventoryService] 收到庫存預留請求: {}", command.getOrderId());

        // 模擬業務規則判定
        if ("OUT_OF_STOCK".equals(command.getProductId())) {
            log.warn("[InventoryService] 庫存不足!");
            eventGateway.publish(new StockFailedEvent(command.getOrderId(), "庫存不足"));
        } else {
            log.info("[InventoryService] 庫存扣除成功");
            eventGateway.publish(new StockReservedEvent(command.getOrderId(), command.getProductId()));
        }
    }
}
