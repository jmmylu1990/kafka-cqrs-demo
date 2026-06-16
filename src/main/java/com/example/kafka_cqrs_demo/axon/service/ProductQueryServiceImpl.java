package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.dto.ProductStockDto;
import com.example.kafka_cqrs_demo.axon.repository.AxonInventoryRepository;
import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 商品庫存查詢服務實作類別 (Product Query Service Implementation)
 */
@Slf4j
@Service
public class ProductQueryServiceImpl implements ProductQueryService {

    private final RedissonClient redissonClient;
    private final AxonInventoryRepository inventoryRepository;
    private final MeterRegistry meterRegistry;

    public ProductQueryServiceImpl(RedissonClient redissonClient,
                                   AxonInventoryRepository inventoryRepository,
                                   MeterRegistry meterRegistry) {
        this.redissonClient = redissonClient;
        this.inventoryRepository = inventoryRepository;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public ProductStockDto getProductStock(String productId) {
        String stockKey = "{product:" + productId + "}:stock";
        String reservedKey = "{product:" + productId + "}:reserved";

        RBucket<String> stockBucket = redissonClient.getBucket(stockKey, StringCodec.INSTANCE);
        RBucket<String> reservedBucket = redissonClient.getBucket(reservedKey, StringCodec.INSTANCE);

        // 1. 嘗試直接從 Redis 讀取
        String stockVal = stockBucket.get();
        String reservedVal = reservedBucket.get();

        if (stockVal != null) {
            meterRegistry.counter("cache.requests", "type", "product", "result", "hit").increment();
        } else {
            // 2. 獲取 Redis 分散式鎖，防範快取擊穿
            String lockKey = "{product:" + productId + "}:lock:query";
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

                            boolean isHot = inventory.isHot();
                            long ttl = isHot ? 1 : 60;
                            TimeUnit timeUnit = isHot ? TimeUnit.DAYS : TimeUnit.SECONDS;

                            stockBucket.set(stockVal, ttl, timeUnit);
                            reservedBucket.set(reservedVal, ttl, timeUnit);

                            String isHotKey = "{product:" + productId + "}:isHot";
                            redissonClient.getBucket(isHotKey, StringCodec.INSTANCE).set(isHot ? "true" : "false", ttl, timeUnit);

                            log.info("[DCL-Product] 從 MySQL 讀取成功並已回寫 Redis 快取 (是否為熱商品: {}, TTL: {} {}): {}",
                                    isHot, ttl, timeUnit, productId);
                            meterRegistry.counter("cache.requests", "type", "product", "result", "miss").increment();
                        } else {
                            log.warn("[DCL-Product] 商品不存在，ID: {}", productId);
                            throw new IllegalArgumentException("商品不存在");
                        }
                    } else {
                        log.info("[DCL-Product] 雙重檢查命中商品快取 (Double-Checked Cache Hit): {}", productId);
                        meterRegistry.counter("cache.requests", "type", "product", "result", "hit").increment();
                    }
                } else {
                    // 獲取鎖超時，退避並嘗試重新讀取 Redis
                    log.warn("[DCL-Product] 獲取商品查詢鎖超時，嘗試直接讀取 Redis 快取: {}", productId);
                    Thread.sleep(100);
                    stockVal = stockBucket.get();
                    reservedVal = reservedBucket.get();
                    if (stockVal == null) {
                        throw new IllegalStateException("系統繁忙，請稍候再試");
                    }
                    meterRegistry.counter("cache.requests", "type", "product", "result", "hit").increment();
                }
            } catch (InterruptedException e) {
                log.error("[DCL-Product] 獲取鎖執行緒被中斷: {}", e.getMessage(), e);
                Thread.currentThread().interrupt();
                throw new IllegalStateException("系統繁忙，請稍候再試");
            } catch (IllegalArgumentException | IllegalStateException ex) {
                throw ex;
            } catch (Exception e) {
                log.error("[DCL-Product] 查詢商品庫存出現異常: {}", e.getMessage(), e);
                throw new RuntimeException("商品庫存查詢失敗");
            } finally {
                if (locked && lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }

        int stock = Integer.parseInt(stockVal);
        int reserved = (reservedVal != null) ? Integer.parseInt(reservedVal) : 0;
        return new ProductStockDto(productId, stock, reserved);
    }
}
