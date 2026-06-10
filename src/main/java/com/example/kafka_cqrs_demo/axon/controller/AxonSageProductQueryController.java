package com.example.kafka_cqrs_demo.axon.controller;

import com.example.kafka_cqrs_demo.axon.repository.AxonInventoryRepository;
import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 商品庫存查詢端 API 控制器 (Product Query Controller)
 * <p>
 * 提供商品庫存的 Cache-Aside 查詢端點，並實作 Double-Checked Locking 防範快取擊穿。
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("/axonsaga/api/products")
public class AxonSageProductQueryController {

    private final RedissonClient redissonClient;
    private final AxonInventoryRepository inventoryRepository;

    public AxonSageProductQueryController(RedissonClient redissonClient,
                                           AxonInventoryRepository inventoryRepository) {
        this.redissonClient = redissonClient;
        this.inventoryRepository = inventoryRepository;
    }

    /**
     * 查詢特定商品的可用庫存與預留庫存。
     *
     * @param productId 商品 ID
     * @return 包含庫存資訊的響應實體，若不存在則回傳 404
     */
    @GetMapping("/{productId}/stock")
    public ResponseEntity<ProductStockDto> getProductStock(@PathVariable String productId) {
        log.info("查詢商品庫存請求，ID: {}", productId);

        String stockKey = "product:" + productId + ":stock";
        String reservedKey = "product:" + productId + ":reserved";

        RBucket<String> stockBucket = redissonClient.getBucket(stockKey, StringCodec.INSTANCE);
        RBucket<String> reservedBucket = redissonClient.getBucket(reservedKey, StringCodec.INSTANCE);

        // 1. 嘗試直接從 Redis 讀取
        String stockVal = stockBucket.get();
        String reservedVal = reservedBucket.get();

        if (stockVal == null) {
            // 2. 獲取 Redis 分散式鎖，防範快取擊穿
            String lockKey = "lock:product:query:" + productId;
            RLock lock = redissonClient.getLock(lockKey);
            boolean locked = false;
            try {
                // 嘗試獲取鎖，最多等待 2 秒，鎖持有 5 秒
                locked = lock.tryLock(2, 5, TimeUnit.SECONDS);
                if (locked) {
                    // 雙重檢查 (Double Check)：再次從 Redis 取得
                    stockVal = stockBucket.get();
                    reservedVal = reservedBucket.get();

                    if (stockVal == null) {
                        log.info("[DCL-Product] Redis 快取未命中且已獲取鎖，嘗試從 MySQL 查詢商品: {}", productId);
                        Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(productId);
                        if (inventoryOpt.isPresent()) {
                            AxonInventoryEntity inventory = inventoryOpt.get();
                            stockVal = String.valueOf(inventory.getStock());
                            reservedVal = String.valueOf(inventory.getReservedStock());

                            // 寫回 Redis，設定 TTL 為 30 分鐘（模擬冷熱分離的冷商品動態加載）
                            stockBucket.set(stockVal, 30, TimeUnit.MINUTES);
                            reservedBucket.set(reservedVal, 30, TimeUnit.MINUTES);
                            log.info("[DCL-Product] 從 MySQL 讀取成功並已回寫 Redis 快取 (TTL 30分鐘): {}", productId);
                        } else {
                            log.warn("[DCL-Product] 商品不存在，ID: {}", productId);
                            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
                        }
                    } else {
                        log.info("[DCL-Product] 雙重檢查命中商品快取 (Double-Checked Cache Hit): {}", productId);
                    }
                } else {
                    // 獲取鎖超時，退避並嘗試重新讀取 Redis
                    log.warn("[DCL-Product] 獲取商品查詢鎖超時，嘗試直接讀取 Redis 快取: {}", productId);
                    Thread.sleep(100);
                    stockVal = stockBucket.get();
                    reservedVal = reservedBucket.get();
                    if (stockVal == null) {
                        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build();
                    }
                }
            } catch (InterruptedException e) {
                log.error("[DCL-Product] 獲取鎖執行緒被中斷: {}", e.getMessage(), e);
                Thread.currentThread().interrupt();
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            } catch (Exception e) {
                log.error("[DCL-Product] 查詢商品庫存出現異常: {}", e.getMessage(), e);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            } finally {
                if (locked && lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }

        int stock = Integer.parseInt(stockVal);
        int reserved = (reservedVal != null) ? Integer.parseInt(reservedVal) : 0;
        return ResponseEntity.ok(new ProductStockDto(productId, stock, reserved));
    }

    /**
     * 商品庫存傳輸物件 (DTO)
     */
    public static class ProductStockDto {
        private final String productId;
        private final int stock;
        private final int reservedStock;

        public ProductStockDto(String productId, int stock, int reservedStock) {
            this.productId = productId;
            this.stock = stock;
            this.reservedStock = reservedStock;
        }

        public String getProductId() {
            return productId;
        }

        public int getStock() {
            return stock;
        }

        public int getReservedStock() {
            return reservedStock;
        }
    }
}
