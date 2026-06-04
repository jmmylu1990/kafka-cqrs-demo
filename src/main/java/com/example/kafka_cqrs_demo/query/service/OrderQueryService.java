package com.example.kafka_cqrs_demo.query.service;

import com.example.kafka_cqrs_demo.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
@Slf4j
public class OrderQueryService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    // 定義一個真正屬於 Query 端持久化/長效的 Redis Key 前綴
    private static final String REDIS_VIEW_PREFIX = "order:view:";

    public OrderQueryService(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 當 Consumer 收到 Kafka 事件時，呼叫此方法「真正寫入 Redis 唯讀資料庫」
     */
    public void syncOrderView(OrderCreatedEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            String redisKey = REDIS_VIEW_PREFIX + event.getOrderId();

            // 這裡代表讀取端的「真實資料庫」，所以我們「不設定過期時間（TTL）」，讓它永久保存
            redisTemplate.opsForValue().set(redisKey, json);

            log.info("【Query 端】Redis 唯讀視圖（Read View）已同步更新，Order ID: {}", event.getOrderId());
        } catch (Exception e) {
            log.error("同步 Redis 唯讀視圖失敗", e);
        }
    }

    /**
     * 提供給 Controller 查詢使用
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