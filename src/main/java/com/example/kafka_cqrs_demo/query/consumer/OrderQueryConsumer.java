package com.example.kafka_cqrs_demo.query.consumer;

import com.example.kafka_cqrs_demo.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.query.service.OrderQueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class OrderQueryConsumer {

    private final OrderQueryService orderQueryService;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    public OrderQueryConsumer(OrderQueryService orderQueryService,
                              ObjectMapper objectMapper,
                              StringRedisTemplate redisTemplate) {
        this.orderQueryService = orderQueryService;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(topics = "order-events", groupId = "my-cqrs-group")
    public void handleOrderEvent(String message) {
        try {
            OrderCreatedEvent event = objectMapper.readValue(message, OrderCreatedEvent.class);
            String orderId = event.getOrderId();

            // 高併發核心】分散式冪等性鎖防止重複消費
            // 使用 Redis 的 setIfAbsent (對應 Redis 原生命令 SETNX)
            String lockKey = "order:lock:consume:" + orderId;
            Boolean isFirstConsume = redisTemplate.opsForValue().setIfAbsent(lockKey, "LOCKED", 10, TimeUnit.MINUTES);

            if (Boolean.FALSE.equals(isFirstConsume)) {
                log.warn("【Query 端警告】檢測到重複發送的 Kafka 事件，系統已主動攔截！Order ID: {}", orderId);
                return; // 直接攔截，不再重複更新唯讀視圖，確保冪等
            }

            log.info("【Query 端】成功獲取消費鎖，開始消費訊息: {}", message);

            // 呼叫 Service 更新 Redis 唯讀長效視圖 (order:view:)
            orderQueryService.syncOrderView(event);

        } catch (Exception e) {
            log.error("【Query 端】解析 Kafka 事件失敗", e);
        }
    }
}