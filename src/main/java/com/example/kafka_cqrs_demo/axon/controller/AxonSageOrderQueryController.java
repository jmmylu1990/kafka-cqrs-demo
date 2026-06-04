package com.example.kafka_cqrs_demo.axon.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 查詢控制器 (Query Controller)
 * CQRS 的核心在於讀寫分離。
 * 與 CommandController 不同，這裡完全不依賴 Axon Gateway，
 * 而是直接從「投影層 (Read Model)」讀取資料，實現極致的讀取效能。
 */
@Slf4j
@RestController
@RequestMapping("/axonsaga/api/orders")
public class AxonSageOrderQueryController {
    private final StringRedisTemplate redisTemplate;

    public AxonSageOrderQueryController(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 根據訂單 ID 查詢訂單詳情。
     * 流程：前端請求 -> Controller -> Redis -> 返回 JSON。
     * 此流程不經過 Axon 事件總線，因此速度極快。
     */
    @GetMapping("/{orderId}")
    public String getOrder(@PathVariable String orderId) {
        log.info("🔍 查詢訂單請求，ID: {}", orderId);

        // 從 Redis 取出之前由 OrderViewProjector 預先同步好的 JSON 資料
        String orderJson = redisTemplate.opsForValue().get("order:" + orderId);

        if (orderJson == null) {
            log.warn("⚠訂單不存在，ID: {}", orderId);
            return "訂單不存在";
        }

        log.info("✅ 查詢成功，返回訂單資料");
        return orderJson;
    }
}
