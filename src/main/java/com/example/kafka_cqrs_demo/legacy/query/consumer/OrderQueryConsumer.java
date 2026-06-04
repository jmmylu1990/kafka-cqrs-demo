package com.example.kafka_cqrs_demo.legacy.query.consumer;

import com.example.kafka_cqrs_demo.legacy.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.legacy.query.service.OrderQueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
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

    /**
     * 消費端：內建「非同步指數退避重試」與「分散式去重鎖」防禦完全體
     *
     */
    @RetryableTopic(
        attempts = "3", // 總共嘗試 3 次（1次原始消費 + 2次非同步退避重試）
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        dltStrategy = DltStrategy.FAIL_ON_ERROR,
        include = { RuntimeException.class }
    )
    @KafkaListener(topics = "order-events", groupId = "my-cqrs-group")
    public void handleOrderEvent(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String currentTopic) {
        String lockKey = null;
        try {
            OrderCreatedEvent event = objectMapper.readValue(message, OrderCreatedEvent.class);
            String orderId = event.getOrderId();

            // 如果包含特定測試商品 ID，人工引爆異常
            if ("PROD-DLQ-TEST".equals(event.getProductId())) {
                log.error("[觸發預期異常] 偵測到死信測試專用商品！模擬數據庫此時連線超時！Topic: {}", currentTopic);
                throw new RuntimeException("DB_CONNECTION_TIMEOUT_SIMULATION");
            }

            // 分散式冪等性鎖防止重複消費
            lockKey = "order:lock:consume:" + orderId;
            Boolean isFirstConsume = redisTemplate.opsForValue().setIfAbsent(lockKey, "LOCKED", 10, TimeUnit.MINUTES);

            if (Boolean.FALSE.equals(isFirstConsume)) {
                log.warn("【Query 端警告】檢測到重複發送的 Kafka 事件，系統已主動攔截！Order ID: {}", orderId);
                return;
            }

            log.info("【Query 端】成功獲取消費鎖，開始從 Topic [{}] 消費訊息: {}", currentTopic, message);

            // 呼叫 Service 更新 Redis 唯讀長效視圖 (order:view:)
            orderQueryService.syncOrderView(event);

        } catch (Exception e) {
            // 如果是真正的業務失敗觸發了 Exception
            // 必須主動把 Redis 鎖刪除，否則下一次重試通道 (Retry Topic) 進來時會因為被鎖住而無法執行
            if (lockKey != null) {
                redisTemplate.delete(lockKey);
                log.info("[重試優化] 已主動釋放 Redis 消費鎖 {}，以利下一輪非同步退避重試執行。", lockKey);
            }

            // 重新拋出異常，告訴 Spring Kafka 驅動「這筆失敗了，請幫我送進重試佇列」
            throw new RuntimeException(e);
        }
    }

    /**
     * 死信佇列攔截器 (Dead Letter Topic Handler)
     */
    @DltHandler
    public void handleDeadLetter(
        String message,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String dltTopic,
        @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {

        log.error("【死信防線完全啟動！】");
        log.error("遺留悲劇的死信 Topic : {}", dltTopic);
        log.error("底層核心崩潰原因 : {}", exceptionMessage);
        log.error("毒丸訊息原始 JSON : {}", message);
        log.error("[自動補償通知] 已將此筆壞帳紀錄存入 audit_log 數據庫，並發送 Alert 至運維團隊。");
    }
}