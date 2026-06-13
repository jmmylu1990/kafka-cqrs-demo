package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.repository.AxonInventoryRepository;
import com.example.kafka_cqrs_demo.axon.repository.AxonStockReservationRepository;
import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;

/**
 * 庫存同步處理策略抽象基類 (Abstract Inventory Sync Handler)
 * <p>
 * 共享資料庫 Repository 依賴與庫存更新樂觀鎖重試邏輯，避免樣板程式碼重複。
 * </p>
 */
@Slf4j
public abstract class AbstractInventorySyncHandler implements InventorySyncHandler {

    @Autowired
    protected AxonInventoryRepository inventoryRepository;

    @Autowired
    protected AxonStockReservationRepository reservationRepository;

    /**
     * 更新商品庫存與預留庫存，拋出樂觀鎖例外以觸發 Kafka 重試機制與全新交易。
     */
    protected void updateInventoryWithRetry(String productId, int stockDelta, int reservedDelta) {
        Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(productId);
        if (inventoryOpt.isPresent()) {
            AxonInventoryEntity inventory = inventoryOpt.get();
            inventory.setStock(Math.max(0, inventory.getStock() + stockDelta));
            inventory.setReservedStock(Math.max(0, inventory.getReservedStock() + reservedDelta));
            // 若發生併發更新，save 將直接拋出 ObjectOptimisticLockingFailureException。
            // 異常會直接往上拋出使當前交易 Rollback，由 Spring Kafka ErrorHandler 觸發整筆訊息的重新消費與全新交易重試。
            inventoryRepository.save(inventory);
        } else {
            log.error("[InventorySync] MySQL 未找到商品 {}，無法同步庫存變更", productId);
        }
    }
}
