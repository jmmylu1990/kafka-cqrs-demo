package com.example.kafka_cqrs_demo.axon.config;

import com.example.kafka_cqrs_demo.axon.repository.AxonInventoryRepository;
import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Axon 庫存數據庫初始化器 (Axon Database Initializer)
 * <p>
 * 當 Spring Boot 應用程式啟動時，檢查 MySQL 數據庫中是否存在預設商品的庫存記錄。
 * 若不存在，則自動插入預設庫存資料，以便於測試與開發：
 * - PROD-001: 擁有 100 件可用庫存 (可用於成功流程)
 * - PROD-002: 擁有 5 件可用庫存 (可用於臨界數量測試)
 * - PROD-003: 擁有 0 件可用庫存 (可用於庫存扣減失敗與 Saga 補償流程測試)
 * </p>
 */
@Component
@Slf4j
public class AxonDatabaseInitializer implements CommandLineRunner {

    private final AxonInventoryRepository inventoryRepository;

    public AxonDatabaseInitializer(AxonInventoryRepository inventoryRepository) {
        this.inventoryRepository = inventoryRepository;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("[AxonDatabaseInitializer] 開始初始化 Axon 庫存測試數據...");

        if (!inventoryRepository.existsById("PROD-001")) {
            inventoryRepository.save(new AxonInventoryEntity("PROD-001", 100, 0));
            log.info("[AxonDatabaseInitializer] 插入商品 PROD-001: 庫存 100 件");
        }

        if (!inventoryRepository.existsById("PROD-002")) {
            inventoryRepository.save(new AxonInventoryEntity("PROD-002", 5, 0));
            log.info("[AxonDatabaseInitializer] 插入商品 PROD-002: 庫存 5 件");
        }

        if (!inventoryRepository.existsById("PROD-003")) {
            inventoryRepository.save(new AxonInventoryEntity("PROD-003", 0, 0));
            log.info("[AxonDatabaseInitializer] 插入商品 PROD-003: 庫存 0 件");
        }

        log.info("[AxonDatabaseInitializer] 庫存測試數據初始化完成。");
    }
}
