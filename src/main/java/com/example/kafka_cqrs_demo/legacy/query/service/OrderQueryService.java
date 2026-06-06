package com.example.kafka_cqrs_demo.legacy.query.service;

import com.example.kafka_cqrs_demo.legacy.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * 傳統模式讀取端查詢服務 (Legacy Order Query Service)
 * <p>
 * 提供傳統 CQRS 模式下的讀取端模型 (Read Model) 同步與查詢功能。
 * 當消費者 {@link com.example.kafka_cqrs_demo.legacy.query.consumer.OrderQueryConsumer} 接收到事件後，
 * 呼叫此服務將唯讀資料寫入 Redis 中長效保存；同時也負責反序列化並向 Controller 提供查詢功能。
 * </p>
 */
@Service
@Slf4j
public class OrderQueryService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    /** 讀取端 Redis 長效視圖的 Key 前綴 */
    private static final String REDIS_VIEW_PREFIX = "order:view:";

    /**
     * 建構子。
     *
     * @param redisTemplate Spring Redis 模板
     * @param objectMapper Jackson ObjectMapper 實例
     */
    public OrderQueryService(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 將 Kafka 事件反序列化並同步寫入 Redis 唯讀長效資料庫。
     * <p>
     * <b>設計說明：</b>
     * 此處代表讀取端 (Read Model) 的真實數據儲存庫，因此不設定快取過期時間 (TTL)，讓其永久保存以提供穩定的讀取。
     * </p>
     *
     * @param event 傳統模式下的訂單建立事件
     */
    public void syncOrderView(OrderCreatedEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            String redisKey = REDIS_VIEW_PREFIX + event.getOrderId();

            redisTemplate.opsForValue().set(redisKey, json);
            log.info("【Query 端】Redis 唯讀視圖（Read View）已同步更新，Order ID: {}", event.getOrderId());
        } catch (Exception e) {
            log.error("同步 Redis 唯讀視圖失敗", e);
        }
    }

    /**
     * 根據訂單唯一識別碼從 Redis 唯讀視圖中查詢訂單事件資料。
     *
     * @param orderId 訂單唯一識別碼
     * @return 訂單建立事件物件，若不存在或發生異常則返回 null
     */
    public OrderCreatedEvent getOrderById(String orderId) {
        String redisKey = REDIS_VIEW_PREFIX + orderId;
        String json = redisTemplate.opsForValue().get(redisKey);

        if (json == null) {
            return null;
        }

        try {
            return objectMapper.readValue(json, OrderCreatedEvent.class);
        } catch (Exception e) {
            log.error("反序列化訂單資料失敗", e);
            return null;
        }
    }
}