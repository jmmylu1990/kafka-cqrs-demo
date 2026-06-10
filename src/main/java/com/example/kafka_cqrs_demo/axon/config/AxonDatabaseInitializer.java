package com.example.kafka_cqrs_demo.axon.config;

import com.example.kafka_cqrs_demo.axon.repository.AxonInventoryRepository;
import com.example.kafka_cqrs_demo.axon.repository.AxonWalletRepository;
import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
import com.example.kafka_cqrs_demo.entity.AxonWalletEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Axon 庫存與錢包數據庫初始化器 (Axon Database Initializer)
 * <p>
 * 當 Spring Boot 應用程式啟動時，檢查 MySQL 與 Redis 數據庫中是否存在預設商品及錢包記錄。
 * 並在 Redis 中設定快取，用於高併發扣減。
 * </p>
 */
@Component
@Slf4j
public class AxonDatabaseInitializer implements CommandLineRunner {

    private final AxonInventoryRepository inventoryRepository;
    private final AxonWalletRepository walletRepository;
    private final StringRedisTemplate redisTemplate;

    public AxonDatabaseInitializer(AxonInventoryRepository inventoryRepository,
                                   AxonWalletRepository walletRepository,
                                   StringRedisTemplate redisTemplate) {
        this.inventoryRepository = inventoryRepository;
        this.walletRepository = walletRepository;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("[AxonDatabaseInitializer] 開始初始化 Axon 庫存測試數據 (MySQL & Redis)...");

        // PROD-001 (熱點商品 - 預熱快取)
        if (!inventoryRepository.existsById("PROD-001")) {
            inventoryRepository.save(new AxonInventoryEntity("PROD-001", 100, 0, true));
            log.info("[AxonDatabaseInitializer] 插入熱點商品 PROD-001: 庫存 100 件");
        } else {
            // 確保資料庫中的 isHot 為 true
            inventoryRepository.findById("PROD-001").ifPresent(entity -> {
                entity.setHot(true);
                inventoryRepository.save(entity);
            });
        }
        redisTemplate.opsForValue().set("product:PROD-001:stock", "100", 1, TimeUnit.DAYS);
        redisTemplate.opsForValue().set("product:PROD-001:reserved", "0", 1, TimeUnit.DAYS);
        redisTemplate.opsForValue().set("product:PROD-001:isHot", "true", 1, TimeUnit.DAYS);

        // PROD-002 (冷商品 - 不預熱快取，清除快取以防殘留)
        if (!inventoryRepository.existsById("PROD-002")) {
            inventoryRepository.save(new AxonInventoryEntity("PROD-002", 5, 0, false));
            log.info("[AxonDatabaseInitializer] 插入冷商品 PROD-002: 庫存 5 件");
        } else {
            inventoryRepository.findById("PROD-002").ifPresent(entity -> {
                entity.setHot(false);
                inventoryRepository.save(entity);
            });
        }
        redisTemplate.delete("product:PROD-002:stock");
        redisTemplate.delete("product:PROD-002:reserved");
        redisTemplate.delete("product:PROD-002:isHot");

        // PROD-003 (冷商品 - 不預熱快取，清除快取以防殘留)
        if (!inventoryRepository.existsById("PROD-003")) {
            inventoryRepository.save(new AxonInventoryEntity("PROD-003", 0, 0, false));
            log.info("[AxonDatabaseInitializer] 插入冷商品 PROD-003: 庫存 0 件");
        } else {
            inventoryRepository.findById("PROD-003").ifPresent(entity -> {
                entity.setHot(false);
                inventoryRepository.save(entity);
            });
        }
        redisTemplate.delete("product:PROD-003:stock");
        redisTemplate.delete("product:PROD-003:reserved");
        redisTemplate.delete("product:PROD-003:isHot");

        // PROD-DLQ-TEST (熱點商品 - 預熱快取)
        if (!inventoryRepository.existsById("PROD-DLQ-TEST")) {
            inventoryRepository.save(new AxonInventoryEntity("PROD-DLQ-TEST", 100, 0, true));
            log.info("[AxonDatabaseInitializer] 插入熱點測試商品 PROD-DLQ-TEST: 庫存 100 件");
        } else {
            inventoryRepository.findById("PROD-DLQ-TEST").ifPresent(entity -> {
                entity.setHot(true);
                inventoryRepository.save(entity);
            });
        }
        redisTemplate.opsForValue().set("product:PROD-DLQ-TEST:stock", "100", 1, TimeUnit.DAYS);
        redisTemplate.opsForValue().set("product:PROD-DLQ-TEST:reserved", "0", 1, TimeUnit.DAYS);
        redisTemplate.opsForValue().set("product:PROD-DLQ-TEST:isHot", "true", 1, TimeUnit.DAYS);

        log.info("[AxonDatabaseInitializer] 開始初始化 Axon 錢包測試數據...");

        if (!walletRepository.existsById("USER-001")) {
            walletRepository.save(new AxonWalletEntity("USER-001", 1000L));
            log.info("[AxonDatabaseInitializer] 插入使用者 USER-001 錢包: 餘額 1000 元");
        }

        if (!walletRepository.existsById("USER-002")) {
            walletRepository.save(new AxonWalletEntity("USER-002", 10L));
            log.info("[AxonDatabaseInitializer] 插入使用者 USER-002 錢包: 餘額 10 元");
        }

        log.info("[AxonDatabaseInitializer] 測試數據初始化完成。");
    }
}
