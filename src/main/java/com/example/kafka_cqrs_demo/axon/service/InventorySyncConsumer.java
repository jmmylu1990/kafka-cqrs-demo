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
    public void consume(String message) throws Exception {
        log.info("[InventorySyncConsumer] 收到庫存同步 Kafka 訊息: {}", message);

        // 不在此處捕捉 DB/業務異常，讓異常向上拋出以觸發 Spring Kafka 重試與 DLQ 流程
        InventorySyncEvent event = objectMapper.readValue(message, InventorySyncEvent.class);

        // 模擬資料庫暫時性連線失敗異常，用於驗證重試與 DLQ (僅對特定測試商品 PROD-DLQ-TEST 觸發)
        if ("RESERVE".equals(event.getActionType()) && "PROD-DLQ-TEST".equals(event.getProductId())) {
            throw new RuntimeException("模擬資料庫暫時性連線失敗");
        }
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
    }

    /**
     * 監聽死信佇列 (DLQ)，模擬發送警報簡訊通知工程師人工排查。
     *
     * @param message 進入死信佇列的原始訊息內容
     */
    @KafkaListener(topics = InventoryKafkaConfig.INVENTORY_SYNC_DLQ_TOPIC, groupId = "inventory-dlq-group")
    public void consumeDlq(String message) {
        log.error("[ALERT] [SMS GATEWAY] 警報：庫存同步訊息進入死信佇列 (DLQ)！已發送簡訊通知工程師人工排查。訊息內容: {}", message);
    }

    private void handleReserve(InventorySyncEvent event) {
        // 1. 更新商品庫存 (具備樂觀鎖重試機制)
        updateInventoryWithRetry(event.getProductId(), -event.getQuantity(), event.getQuantity());

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

                // 扣除預留庫存 (具備樂觀鎖重試機制)
                updateInventoryWithRetry(reservation.getProductId(), 0, -reservation.getQuantity());
                log.info("[InventorySyncConsumer] MySQL 已同步完成扣減 (COMMIT): orderId={}", event.getOrderId());
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

                // 退回可用庫存，扣減預留庫存 (具備樂觀鎖重試機制)
                updateInventoryWithRetry(reservation.getProductId(), reservation.getQuantity(), -reservation.getQuantity());
                log.info("[InventorySyncConsumer] MySQL 已同步釋放預留 (RELEASE): orderId={}", event.getOrderId());
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

                // 退還可用庫存 (具備樂觀鎖重試機制)
                updateInventoryWithRetry(reservation.getProductId(), reservation.getQuantity(), 0);
                log.info("[InventorySyncConsumer] MySQL 已同步退款庫存 (REFUND): orderId={}", event.getOrderId());
            } else {
                log.info("[InventorySyncConsumer] MySQL 訂單 {} 預留狀態非 COMPLETED ({})，忽略 REFUND",
                        event.getOrderId(), reservation.getStatus());
            }
        } else {
            log.warn("[InventorySyncConsumer] MySQL 未找到訂單 {} 的預留記錄，無法執行 REFUND 同步", event.getOrderId());
        }
    }

    /**
     * 更新商品庫存與預留庫存，拋出樂觀鎖例外以觸發 Kafka 重試機制與全新交易。
     */
    private void updateInventoryWithRetry(String productId, int stockDelta, int reservedDelta) {
        Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(productId);
        if (inventoryOpt.isPresent()) {
            AxonInventoryEntity inventory = inventoryOpt.get();
            inventory.setStock(Math.max(0, inventory.getStock() + stockDelta));
            inventory.setReservedStock(Math.max(0, inventory.getReservedStock() + reservedDelta));
            // 若發生併發更新，save 將直接拋出 ObjectOptimisticLockingFailureException。
            // 異常會直接往上拋出使當前交易 Rollback，由 Spring Kafka ErrorHandler 觸發整筆訊息的重新消費與全新交易重試。
            inventoryRepository.save(inventory);
        } else {
            log.error("[InventorySyncConsumer] MySQL 未找到商品 {}，無法同步庫存變更", productId);
        }
    }
}
