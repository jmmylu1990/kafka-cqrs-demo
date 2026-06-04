package com.example.kafka_cqrs_demo.command.service;

import com.example.kafka_cqrs_demo.command.domain.OrderEntity;
import com.example.kafka_cqrs_demo.command.repository.OrderCommandRepository;
import com.example.kafka_cqrs_demo.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class OrderCommandService {

    private final OrderCommandRepository orderRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;
    @Lazy
    @Autowired
    private OrderCommandService self;

    @Autowired
    public OrderCommandService(OrderCommandRepository orderRepository,
                               KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               StringRedisTemplate redisTemplate) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
    }

    /**
     * 主流程控制：不加 @Transactional，防止連線池被 Kafka 拖垮
     */
    public String createOrder(String productId, int quantity, long price) {
        String orderId = UUID.randomUUID().toString();

        // 1. 第一步：安全的寫入實體 MySQL (此方法內建事務)
        self.saveOrderToDb(orderId, productId, quantity, price);

        // 2. 第二步：非同步向外廣播與快取
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, productId, quantity, price);
        try {
            String jsonEvent = objectMapper.writeValueAsString(event);

            // 寫入短效快取
            redisTemplate.opsForValue().set("order:cache:" + orderId, jsonEvent, 5, TimeUnit.SECONDS);

            // 發送事件到 Kafka
            kafkaTemplate.send("order-events", orderId, jsonEvent);
            log.info("【Command 端】資料庫已留痕，成功外發 Kafka 事件與 Redis 快取。ID: {}", orderId);
        } catch (Exception e) {
            // 實務考量：萬一 Kafka 失敗，MySQL 已經有了 PENDING 資料，可以透過排程（Cron Job）進行補償重發
            log.error("【Command 嚴重警告】Kafka 發送失敗，但 DB 已留痕，觸發異步補償機制", e);
        }

        return orderId;
    }

    @Transactional
    public void saveOrderToDb(String orderId, String productId, int quantity, long price) {
        OrderEntity entity = new OrderEntity();
        entity.setOrderId(orderId);
        entity.setProductId(productId);
        entity.setQuantity(quantity);
        entity.setPrice(price);
        entity.setStatus("PENDING"); // 初始化狀態
        entity.setCreateTime(LocalDateTime.now());
        orderRepository.save(entity);
    }
}