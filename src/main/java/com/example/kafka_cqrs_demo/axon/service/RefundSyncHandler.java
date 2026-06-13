package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.dto.InventorySyncEvent;
import com.example.kafka_cqrs_demo.entity.AxonStockReservationEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * 退款庫存同步處理器 (Refund Sync Handler)
 * <p>
 * 處理 REFUND 動作：更新預留明細為 REFUNDED，並退回可用庫存。
 * </p>
 */
@Slf4j
@Component("REFUND")
public class RefundSyncHandler extends AbstractInventorySyncHandler {

    @Override
    public void handle(InventorySyncEvent event) {
        Optional<AxonStockReservationEntity> reservationOpt = reservationRepository.findById(event.getOrderId());
        if (reservationOpt.isPresent()) {
            AxonStockReservationEntity reservation = reservationOpt.get();
            if ("COMPLETED".equals(reservation.getStatus())) {
                // 更新狀態為已退款
                reservation.setStatus("REFUNDED");
                reservation.setUpdateTime(LocalDateTime.now());
                reservationRepository.save(reservation);

                // 退還可用庫存 (具備樂觀鎖重試機制)
                updateInventoryWithRetry(reservation.getProductId(), reservation.getQuantity(), 0);
                log.info("[RefundSyncHandler] MySQL 已同步退款庫存 (REFUND): orderId={}", event.getOrderId());
            } else {
                log.info("[RefundSyncHandler] MySQL 訂單 {} 預留狀態非 COMPLETED ({})，忽略 REFUND",
                        event.getOrderId(), reservation.getStatus());
            }
        } else {
            log.warn("[RefundSyncHandler] MySQL 未找到訂單 {} 的預留記錄，無法執行 REFUND 同步", event.getOrderId());
        }
    }
}
