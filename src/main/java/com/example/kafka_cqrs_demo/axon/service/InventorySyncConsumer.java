package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.config.InventoryKafkaConfig;
import com.example.kafka_cqrs_demo.axon.dto.InventorySyncEvent;
import com.example.kafka_cqrs_demo.axon.repository.AxonInventoryRepository;
import com.example.kafka_cqrs_demo.axon.repository.AxonStockReservationRepository;
import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
import com.example.kafka_cqrs_demo.entity.AxonStockReservationEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * 庫存非同步同步消費者 (Inventory Sync Consumer)
 * <p>
 * 負責監聽 Kafka 中的 {@code inventory-sync-events} 主題。
 * 當 Redis 執行高併發庫存預留/確認/釋放/退款後，此消費者將收到的事件批次/非同步地寫回 MySQL，
 * 確保關係型資料庫的最終一致性，用於持久化與報表審計。
 * </p>
 */
@Slf4j
@Service
public class InventorySyncConsumer {

    private final AxonInventoryRepository inventoryRepository;
    private final AxonStockReservationRepository reservationRepository;
    private final ObjectMapper objectMapper;

    public InventorySyncConsumer(AxonInventoryRepository inventoryRepository,
                                 AxonStockReservationRepository reservationRepository,
                                 ObjectMapper objectMapper) {
        this.inventoryRepository = inventoryRepository;
        this.reservationRepository = reservationRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * 監聽並處理庫存同步 Kafka 訊息。
     *
     * @param message Kafka 收到的 JSON 字串訊息
     */
    @KafkaListener(topics = InventoryKafkaConfig.INVENTORY_SYNC_TOPIC, groupId = "inventory-sync-group")
    @Transactional
    public void consume(String message) {
        log.info("[InventorySyncConsumer] 收到庫存同步 Kafka 訊息: {}", message);

        try {
            InventorySyncEvent event = objectMapper.readValue(message, InventorySyncEvent.class);
            log.info("[InventorySyncConsumer] 解析事件成功: orderId={}, action={}", event.getOrderId(), event.getActionType());

            switch (event.getActionType()) {
                case "RESERVE":
                    handleReserve(event);
                    break;
                case "COMMIT":
                    handleCommit(event);
                    break;
                case "RELEASE":
                    handleRelease(event);
                    break;
                case "REFUND":
                    handleRefund(event);
                    break;
                default:
                    log.warn("[InventorySyncConsumer] 未知的同步動作類別: {}", event.getActionType());
            }
        } catch (Exception e) {
            log.error("[InventorySyncConsumer] 處理庫存同步訊息失敗: {}", e.getMessage(), e);
        }
    }

    private void handleReserve(InventorySyncEvent event) {
        // 1. 更新商品庫存
        Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(event.getProductId());
        if (inventoryOpt.isPresent()) {
            AxonInventoryEntity inventory = inventoryOpt.get();
            inventory.setStock(Math.max(0, inventory.getStock() - event.getQuantity()));
            inventory.setReservedStock(inventory.getReservedStock() + event.getQuantity());
            inventoryRepository.save(inventory);
            log.info("[InventorySyncConsumer] MySQL 已同步庫存扣減 (RESERVE): product={}, stock={}, reserved={}",
                    event.getProductId(), inventory.getStock(), inventory.getReservedStock());
        } else {
            log.error("[InventorySyncConsumer] MySQL 未找到商品 {}，無法同步扣減可用庫存", event.getProductId());
        }

        // 2. 建立預留明細記錄
        AxonStockReservationEntity reservation = new AxonStockReservationEntity(
                event.getOrderId(),
                event.getProductId(),
                event.getQuantity(),
                "RESERVED",
                LocalDateTime.now()
        );
        reservationRepository.save(reservation);
        log.info("[InventorySyncConsumer] MySQL 已建立預留記錄: orderId={}", event.getOrderId());
    }

    private void handleCommit(InventorySyncEvent event) {
        Optional<AxonStockReservationEntity> reservationOpt = reservationRepository.findById(event.getOrderId());
        if (reservationOpt.isPresent()) {
            AxonStockReservationEntity reservation = reservationOpt.get();
            if ("RESERVED".equals(reservation.getStatus())) {
                // 更新狀態為已完成交易
                reservation.setStatus("COMPLETED");
                reservation.setUpdateTime(LocalDateTime.now());
                reservationRepository.save(reservation);

                // 扣除預留庫存
                Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(reservation.getProductId());
                if (inventoryOpt.isPresent()) {
                    AxonInventoryEntity inventory = inventoryOpt.get();
                    inventory.setReservedStock(Math.max(0, inventory.getReservedStock() - reservation.getQuantity()));
                    inventoryRepository.save(inventory);
                    log.info("[InventorySyncConsumer] MySQL 已同步完成扣減 (COMMIT): orderId={}, reserved={}",
                            event.getOrderId(), inventory.getReservedStock());
                }
            } else {
                log.info("[InventorySyncConsumer] MySQL 訂單 {} 預留狀態非 RESERVED ({})，忽略 COMMIT",
                        event.getOrderId(), reservation.getStatus());
            }
        } else {
            log.warn("[InventorySyncConsumer] MySQL 未找到訂單 {} 的預留記錄，無法執行 COMMIT 同步", event.getOrderId());
        }
    }

    private void handleRelease(InventorySyncEvent event) {
        Optional<AxonStockReservationEntity> reservationOpt = reservationRepository.findById(event.getOrderId());
        if (reservationOpt.isPresent()) {
            AxonStockReservationEntity reservation = reservationOpt.get();
            if ("RESERVED".equals(reservation.getStatus())) {
                // 更新狀態為已釋放
                reservation.setStatus("RELEASED");
                reservation.setUpdateTime(LocalDateTime.now());
                reservationRepository.save(reservation);

                // 退回可用庫存，扣減預留庫存
                Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(reservation.getProductId());
                if (inventoryOpt.isPresent()) {
                    AxonInventoryEntity inventory = inventoryOpt.get();
                    inventory.setStock(inventory.getStock() + reservation.getQuantity());
                    inventory.setReservedStock(Math.max(0, inventory.getReservedStock() - reservation.getQuantity()));
                    inventoryRepository.save(inventory);
                    log.info("[InventorySyncConsumer] MySQL 已同步釋放預留 (RELEASE): orderId={}, stock={}, reserved={}",
                            event.getOrderId(), inventory.getStock(), inventory.getReservedStock());
                }
            } else {
                log.info("[InventorySyncConsumer] MySQL 訂單 {} 預留狀態非 RESERVED ({})，忽略 RELEASE",
                        event.getOrderId(), reservation.getStatus());
            }
        } else {
            log.warn("[InventorySyncConsumer] MySQL 未找到訂單 {} 的預留記錄，無法執行 RELEASE 同步", event.getOrderId());
        }
    }

    private void handleRefund(InventorySyncEvent event) {
        Optional<AxonStockReservationEntity> reservationOpt = reservationRepository.findById(event.getOrderId());
        if (reservationOpt.isPresent()) {
            AxonStockReservationEntity reservation = reservationOpt.get();
            if ("COMPLETED".equals(reservation.getStatus())) {
                // 更新狀態為已退款
                reservation.setStatus("REFUNDED");
                reservation.setUpdateTime(LocalDateTime.now());
                reservationRepository.save(reservation);

                // 退還可用庫存
                Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(reservation.getProductId());
                if (inventoryOpt.isPresent()) {
                    AxonInventoryEntity inventory = inventoryOpt.get();
                    inventory.setStock(inventory.getStock() + reservation.getQuantity());
                    inventoryRepository.save(inventory);
                    log.info("[InventorySyncConsumer] MySQL 已同步退款庫存 (REFUND): orderId={}, stock={}",
                            event.getOrderId(), inventory.getStock());
                }
            } else {
                log.info("[InventorySyncConsumer] MySQL 訂單 {} 預留狀態非 COMPLETED ({})，忽略 REFUND",
                        event.getOrderId(), reservation.getStatus());
            }
        } else {
            log.warn("[InventorySyncConsumer] MySQL 未找到訂單 {} 的預留記錄，無法執行 REFUND 同步", event.getOrderId());
        }
    }
}
