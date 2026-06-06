package com.example.kafka_cqrs_demo.axon.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

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

    /** 用於讀取快取資料的 Redis Template */
    private final StringRedisTemplate redisTemplate;

    /**
     * 查詢控制器的建構子。
     *
     * @param redisTemplate Spring Boot 自動配置的 StringRedisTemplate 實例
     */
    public AxonSageOrderQueryController(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 根據訂單唯一識別碼查詢訂單詳情。
     * <p>
     * 流程：前端請求 -> 控制器查詢 Redis 中的特定 Key -> 返回 JSON 字串。
     * 由於資料已被 Projector 預先轉換為最適合查詢的扁平結構，此處無須進行複雜的 SQL Join 或事件重放，速度極快。
     * </p>
     *
     * @param orderId 訂單唯一識別碼 (UUID)
     * @return 訂單的 JSON 詳細資料，若找不到則返回「訂單不存在」
     */
    @GetMapping("/{orderId}")
    public String getOrder(@PathVariable String orderId) {
        log.info("查詢訂單請求，ID: {}", orderId);

        // 從 Redis 取得由 OrderViewProjector 同步維護的 JSON 資訊
        String orderJson = redisTemplate.opsForValue().get("order:" + orderId);

        if (orderJson == null) {
            log.warn("訂單不存在，ID: {}", orderId);
            return "訂單不存在";
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
