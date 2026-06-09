package com.example.kafka_cqrs_demo.axon.controller;

import com.example.kafka_cqrs_demo.axon.repository.AxonOrderRepository;
import com.example.kafka_cqrs_demo.entity.AxonOrderEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
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

    /**
     * 查詢控制器的建構子。
     */
    public AxonSageOrderQueryController(StringRedisTemplate redisTemplate,
                                         AxonOrderRepository orderRepository,
                                         ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.orderRepository = orderRepository;
        this.objectMapper = objectMapper;
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

        if (orderJson == null) {
            log.info("Redis 快取未命中 (Cache Miss)，嘗試從 MySQL 查詢: {}", orderId);
            Optional<AxonOrderEntity> orderOpt = orderRepository.findById(orderId);
            if (orderOpt.isPresent()) {
                try {
                    AxonOrderEntity order = orderOpt.get();
                    orderJson = objectMapper.writeValueAsString(order);
                    // 2. 回寫 Redis，設定 TTL 為 1 天
                    redisTemplate.opsForValue().set(key, orderJson, 1, TimeUnit.DAYS);
                    log.info("從 MySQL 讀取成功並已回寫 Redis 快取 (TTL 1 天): {}", orderId);
                } catch (Exception e) {
                    log.error("序列化訂單或寫入 Redis 失敗: {}", e.getMessage(), e);
                    try {
                        return objectMapper.writeValueAsString(orderOpt.get());
                    } catch (Exception ex) {
                        return "查詢出錯";
                    }
                }
            } else {
                log.warn("訂單不存在，ID: {}", orderId);
                return "訂單不存在";
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
