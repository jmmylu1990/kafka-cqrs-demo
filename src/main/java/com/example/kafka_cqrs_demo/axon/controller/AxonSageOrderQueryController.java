package com.example.kafka_cqrs_demo.axon.controller;

import com.example.kafka_cqrs_demo.axon.repository.AxonOrderRepository;
import com.example.kafka_cqrs_demo.entity.AxonOrderEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 訂單查詢端 API 控制器 (Order Query Controller)
 * <p>
 * 作為 CQRS (Command Query Responsibility Segregation) 架構下的讀取端 (Query Side) 入口。
 * 其核心理念在於「讀寫分離」。與 {@link AxonSageOrderCommandController} 不同，
 * 本控制器完全不涉及 Axon Gateway 或 Event Store 的調用，而是直接向高速的投影快取層 (Redis) 讀取資料。
 * 藉此大幅降低資料庫的查詢負擔，提供極致的高併發讀取效能。
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("/axonsaga/api/orders")
public class AxonSageOrderQueryController {

    private final StringRedisTemplate redisTemplate;
    private final AxonOrderRepository orderRepository;
    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;
    private final io.micrometer.core.instrument.MeterRegistry meterRegistry;

    /**
     * 查詢控制器的建構子。
     */
    public AxonSageOrderQueryController(StringRedisTemplate redisTemplate,
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

    /**
     * 根據訂單唯一識別碼查詢訂單詳情。
     * <p>
     * 流程：前端請求 -> 控制器查詢 Redis 中的特定 Key -> 若未命中則從 MySQL 載入並快取回 Redis (TTL 1 天)。
     * </p>
     *
     * @param orderId 訂單唯一識別碼 (UUID)
     * @return 訂單的 JSON 詳細資料，若找不到則返回「訂單不存在」
     */
    @GetMapping("/{orderId}")
    public String getOrder(@PathVariable String orderId) {
        log.info("查詢訂單請求，ID: {}", orderId);

        String key = "order:" + orderId;
        // 1. 先從 Redis 取得
        String orderJson = redisTemplate.opsForValue().get(key);

        if (orderJson != null) {
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
                            // 3. 回寫 Redis，設定 TTL 為 1 天
                            redisTemplate.opsForValue().set(key, orderJson, 1, TimeUnit.DAYS);
                            log.info("[DCL] 從 MySQL 讀取成功並已回寫 Redis 快取 (TTL 1 天): {}", orderId);
                            meterRegistry.counter("cache.requests", "type", "order", "result", "miss").increment();
                        } else {
                            log.warn("[DCL] 訂單不存在，ID: {}", orderId);
                            return "訂單不存在";
                        }
                    } else {
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

        log.info("查詢成功，返回訂單資料");
        return orderJson;
    }

    /**
     * 查詢所有已建立的訂單 ID 列表。
     * <p>
     * 藉由讀取 Redis 的 Set 集合 (Key 為 "orders:all")，
     * 快速取得所有系統中存在的訂單識別碼清單，避免耗時的資料庫掃描。
     * </p>
     *
     * @return 包含所有訂單 ID 的 Set 集合
     */
    @GetMapping("/all")
    public Set<String> getAllOrderIds() {
        log.info("查詢所有訂單 ID 列表");
        // 使用 Redis SMEMBERS 指令取得該 Key 下的所有成員
        return redisTemplate.opsForSet().members("orders:all");
    }
}
