package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.dto.InventorySyncEvent;
import com.example.kafka_cqrs_demo.entity.AxonStockReservationEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * 確認庫存扣減同步處理器 (Commit Sync Handler)
 * <p>
 * 處理 COMMIT 動作：更新預留明細為 COMPLETED，並扣減 MySQL 商品預留庫存。
 * </p>
 */
@Slf4j
@Component("COMMIT")
public class CommitSyncHandler extends AbstractInventorySyncHandler {

    @Override
    public void handle(InventorySyncEvent event) {
        Optional<AxonStockReservationEntity> reservationOpt = reservationRepository.findById(event.getOrderId());
        if (reservationOpt.isPresent()) {
            AxonStockReservationEntity reservation = reservationOpt.get();
            if ("RESERVED".equals(reservation.getStatus())) {
                // 更新狀態為已完成交易
                reservation.setStatus("COMPLETED");
                reservation.setUpdateTime(LocalDateTime.now());
                reservationRepository.save(reservation);

                // 扣除預留庫存 (具備樂觀鎖重試機制)
                updateInventoryWithRetry(reservation.getProductId(), 0, -reservation.getQuantity());
                log.info("[CommitSyncHandler] MySQL 已同步完成扣減 (COMMIT): orderId={}", event.getOrderId());
            } else {
                log.info("[CommitSyncHandler] MySQL 訂單 {} 預留狀態非 RESERVED ({})，忽略 COMMIT",
                        event.getOrderId(), reservation.getStatus());
            }
        } else {
            log.warn("[CommitSyncHandler] MySQL 未找到訂單 {} 的預留記錄，無法執行 COMMIT 同步", event.getOrderId());
        }
    }
}
