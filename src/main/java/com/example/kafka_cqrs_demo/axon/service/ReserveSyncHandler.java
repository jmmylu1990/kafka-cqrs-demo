package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.dto.InventorySyncEvent;
import com.example.kafka_cqrs_demo.entity.AxonStockReservationEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * 預留庫存同步處理器 (Reserve Sync Handler)
 * <p>
 * 處理 RESERVE 動作：更新 MySQL 商品可用與預留庫存，並寫入預留明細記錄。
 * </p>
 */
@Slf4j
@Component("RESERVE")
public class ReserveSyncHandler extends AbstractInventorySyncHandler {

    @Override
    public void handle(InventorySyncEvent event) {
        // 1. 更新商品庫存 (具備樂觀鎖重試機制)
        updateInventoryWithRetry(event.getProductId(), -event.getQuantity(), event.getQuantity());

        // 2. 建立預留明細記錄
        AxonStockReservationEntity reservation = new AxonStockReservationEntity(
                event.getOrderId(),
                event.getProductId(),
                event.getQuantity(),
                "RESERVED",
                LocalDateTime.now());
        reservationRepository.save(reservation);
        log.info("[ReserveSyncHandler] MySQL 已建立預留記錄: orderId={}", event.getOrderId());
    }
}
