package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.repository.AxonOrderRepository;
import com.example.kafka_cqrs_demo.entity.AxonOrderEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 訂單查詢服務實作類別 (Order Query Service Implementation)
 * <p>
 * 封裝了 Redis 快取讀寫、Redisson 分散式鎖 DCL 雙重防護、MySQL 資料庫降級查詢、
 * Jackson 序列化與監控指標追蹤等底層基礎設施細節。
 * </p>
 */
@Slf4j
@Service
public class OrderQueryServiceImpl implements OrderQueryService {

    private final StringRedisTemplate redisTemplate;
    private final AxonOrderRepository orderRepository;
    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;
    private final io.micrometer.core.instrument.MeterRegistry meterRegistry;

    public OrderQueryServiceImpl(StringRedisTemplate redisTemplate,
                                 AxonOrderRepository orderRepository,
                                 ObjectMapper objectMapper,
                                 RedissonClient redissonClient,
                                 io.micrometer.core.instrument.MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.orderRepository = orderRepository;
        this.objectMapper = objectMapper;
        this.redissonClient = redissonClient;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public String getOrder(String orderId) {
        log.info("處理查詢訂單業務邏輯，ID: {}", orderId);

        String key = "order:" + orderId;
        // 1. 先從 Redis 取得
        String orderJson = redisTemplate.opsForValue().get(key);

        if (orderJson != null) {
            if ("NULL".equals(orderJson)) {
                log.info("快取命中防穿透標記 (NULL)，訂單 {} 不存在", orderId);
                meterRegistry.counter("cache.requests", "type", "order", "result", "hit").increment();
                return "訂單不存在";
            }
            meterRegistry.counter("cache.requests", "type", "order", "result", "hit").increment();
        } else {
            // 2. 獲取 Redis 分散式鎖，防範快取擊穿
            String lockKey = "lock:order:query:" + orderId;
            RLock lock = redissonClient.getLock(lockKey);
            boolean locked = false;
            try {
                // 嘗試獲取鎖，最多等待 2 秒，鎖持有 5 秒
                locked = lock.tryLock(2, 5, TimeUnit.SECONDS);
                if (locked) {
                    // 雙重檢查 (Double Check)：再次從 Redis 取得快取
                    orderJson = redisTemplate.opsForValue().get(key);
                    if (orderJson == null) {
                        log.info("[DCL] Redis 快取未命中且已獲取鎖，嘗試從 MySQL 查詢: {}", orderId);
                        Optional<AxonOrderEntity> orderOpt = orderRepository.findById(orderId);
                        if (orderOpt.isPresent()) {
                            AxonOrderEntity order = orderOpt.get();
                            orderJson = objectMapper.writeValueAsString(order);
                            // 3. 回寫 Redis，設定 TTL 為 1 天 + 隨機抖動 (24小時 ~ 25小時)
                            long randomMinutes = java.util.concurrent.ThreadLocalRandom.current().nextLong(60);
                            long ttlMinutes = 24 * 60 + randomMinutes;
                            redisTemplate.opsForValue().set(key, orderJson, ttlMinutes, TimeUnit.MINUTES);
                            log.info("[DCL] 從 MySQL 讀取成功並已回寫 Redis 快取 (TTL {} 分鐘): {}", ttlMinutes, orderId);
                            meterRegistry.counter("cache.requests", "type", "order", "result", "miss").increment();
                        } else {
                            log.warn("[DCL] 訂單不存在，寫入防穿透空值快取 (TTL 5 分鐘)，ID: {}", orderId);
                            redisTemplate.opsForValue().set(key, "NULL", 5, TimeUnit.MINUTES);
                            meterRegistry.counter("cache.requests", "type", "order", "result", "miss").increment();
                            return "訂單不存在";
                        }
                    } else {
                        if ("NULL".equals(orderJson)) {
                            log.info("[DCL] 雙重檢查命中防穿透標記 (NULL): {}", orderId);
                            meterRegistry.counter("cache.requests", "type", "order", "result", "hit").increment();
                            return "訂單不存在";
                        }
                        log.info("[DCL] 雙重檢查命中快取 (Double-Checked Cache Hit): {}", orderId);
                        meterRegistry.counter("cache.requests", "type", "order", "result", "hit").increment();
                    }
                } else {
                    // 獲取鎖超時，退避並嘗試重新讀取 Redis
                    log.warn("[DCL] 獲取查詢鎖超時，嘗試直接讀取 Redis 快取: {}", orderId);
                    Thread.sleep(100);
                    orderJson = redisTemplate.opsForValue().get(key);
                    if (orderJson == null) {
                        return "系統繁忙，請稍後再試";
                    }
                    if ("NULL".equals(orderJson)) {
                        log.info("退避重試後命中防穿透標記 (NULL): {}", orderId);
                        meterRegistry.counter("cache.requests", "type", "order", "result", "hit").increment();
                        return "訂單不存在";
                    }
                    meterRegistry.counter("cache.requests", "type", "order", "result", "hit").increment();
                }
            } catch (InterruptedException e) {
                log.error("[DCL] 獲取鎖執行緒被中斷: {}", e.getMessage(), e);
                Thread.currentThread().interrupt();
                return "系統繁忙";
            } catch (Exception e) {
                log.error("[DCL] 查詢出現異常: {}", e.getMessage(), e);
                return "查詢出錯";
            } finally {
                if (locked && lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }

        return orderJson;
    }

    @Override
    public Set<String> getAllOrderIds() {
        log.info("處理查詢所有訂單 ID 列表業務邏輯");
        // 使用 Redis SMEMBERS 指令取得該 Key 下的所有成員
        return redisTemplate.opsForSet().members("orders:all");
    }
}
