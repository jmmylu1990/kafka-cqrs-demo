package com.example.kafka_cqrs_demo.legacy.command.service;

import com.example.kafka_cqrs_demo.legacy.command.domain.OrderEntity;
import com.example.kafka_cqrs_demo.legacy.command.repository.OrderCommandRepository;
import com.example.kafka_cqrs_demo.legacy.event.OrderCreatedEvent;
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

/**
 * 傳統模式訂單寫入服務 (Legacy Order Command Service)
 * <p>
 * 本服務負責傳統 Kafka 模式下的訂單處理流程。
 * 流程包含：產生隨機 UUID、將初始訂單寫入 MySQL 資料庫、將事件寫入 Redis 短期快取，並將 OrderCreatedEvent 推送至 Kafka 供查詢端消費。
 * </p>
 */
@Service
@Slf4j
public class OrderCommandService {

    private final OrderCommandRepository orderRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    /**
     * 自我注入代理物件。
     * 用以解決 Spring AOP 中，同類別內部方法呼叫導致 {@link Transactional} 註解失效的問題。
     * 標註為 @Lazy 以防循環依賴。
     */
    @Lazy
    @Autowired
    private OrderCommandService self;

    /**
     * 建構子。
     *
     * @param orderRepository 訂單寫入端 Repository
     * @param kafkaTemplate Spring Kafka 模板
     * @param objectMapper Jackson ObjectMapper
     * @param redisTemplate Spring Redis 模板
     */
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
     * 建立訂單主控制流程。
     * <p>
     * <b>設計考量：</b>本方法「不加」{@code @Transactional} 註解。
     * 這是為了避免資料庫交易跨越外部 Kafka 網路傳輸，防範因為 Kafka 網路抖動或超時，
     * 導致資料庫連線長時間被佔用，從而拖垮整個系統的資料庫連線池。
     * </p>
     *
     * @param productId 產品識別碼
     * @param quantity 購買數量
     * @param price 單價
     * @return 新產生的訂單 ID
     */
    public String createOrder(String productId, int quantity, long price) {
        String orderId = UUID.randomUUID().toString();

        // 1. 第一步：呼叫具備獨立事務的方法，安全的寫入 MySQL 資料庫
        self.saveOrderToDb(orderId, productId, quantity, price);

        // 2. 第二步：非同步向外廣播與快取
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, productId, quantity, price);
        try {
            String jsonEvent = objectMapper.writeValueAsString(event);

            // 寫入 5 秒的短效快取，防止前端在發布事件的極短時間內輪詢讀不到資料
            redisTemplate.opsForValue().set("order:cache:" + orderId, jsonEvent, 5, TimeUnit.SECONDS);

            // 發送事件到 Kafka 主題
            kafkaTemplate.send("order-events", orderId, jsonEvent);
            log.info("【Command 端】資料庫已留痕，成功外發 Kafka 事件與 Redis 快取。ID: {}", orderId);
        } catch (Exception e) {
            // 實務考量：萬一 Kafka 失敗，MySQL 已經有了 PENDING 資料，可以透過排程（Cron Job）進行補償重發
            log.error("【Command 嚴重警告】Kafka 發送失敗，但 DB 已留痕，觸發異步補償機制", e);
        }

        return orderId;
    }

    /**
     * 將訂單寫入資料庫的獨立事務方法。
     * 標註了 {@link Transactional} 確保資料庫寫入的原子性。
     *
     * @param orderId 訂單識別碼
     * @param productId 產品識別碼
     * @param quantity 數量
     * @param price 價格
     */
    @Transactional
    public void saveOrderToDb(String orderId, String productId, int quantity, long price) {
        OrderEntity entity = new OrderEntity();
        entity.setOrderId(orderId);
        entity.setProductId(productId);
        entity.setQuantity(quantity);
        entity.setPrice(price);
        entity.setStatus("PENDING"); // 初始狀態設為 PENDING，代表處理中
        entity.setCreateTime(LocalDateTime.now());
        orderRepository.save(entity);
    }
}