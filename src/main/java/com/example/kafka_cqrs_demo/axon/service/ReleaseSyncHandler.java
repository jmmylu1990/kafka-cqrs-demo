package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.dto.InventorySyncEvent;
import com.example.kafka_cqrs_demo.entity.AxonStockReservationEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * 釋放預留庫存同步處理器 (Release Sync Handler)
 * <p>
 * 處理 RELEASE 動作：更新預留明細為 RELEASED，並退回可用庫存、扣減 MySQL 商品預留庫存。
 * </p>
 */
@Slf4j
@Component("RELEASE")
public class ReleaseSyncHandler extends AbstractInventorySyncHandler {

    @Override
    public void handle(InventorySyncEvent event) {
        Optional<AxonStockReservationEntity> reservationOpt = reservationRepository.findById(event.getOrderId());
        if (reservationOpt.isPresent()) {
            AxonStockReservationEntity reservation = reservationOpt.get();
            if ("RESERVED".equals(reservation.getStatus())) {
                // 更新狀態為已釋放
                reservation.setStatus("RELEASED");
                reservation.setUpdateTime(LocalDateTime.now());
                reservationRepository.save(reservation);

                // 退回可用庫存，扣減預留庫存 (具備樂觀鎖重試機制)
                updateInventoryWithRetry(reservation.getProductId(), reservation.getQuantity(),
                        -reservation.getQuantity());
                log.info("[ReleaseSyncHandler] MySQL 已同步釋放預留 (RELEASE): orderId={}", event.getOrderId());
            } else {
                log.info("[ReleaseSyncHandler] MySQL 訂單 {} 預留狀態非 RESERVED ({})，忽略 RELEASE",
                        event.getOrderId(), reservation.getStatus());
            }
        } else {
            log.warn("[ReleaseSyncHandler] MySQL 未找到訂單 {} 的預留記錄，無法執行 RELEASE 同步", event.getOrderId());
        }
    }
}
