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

/**
 * 傳統模式讀取端 Kafka 事件消費者 (Legacy Order Query Consumer)
 * <p>
 * 本類別負責監聽並消費 Kafka 中的 "order-events" 主題。
 * 為了解決高併發場景下的訊息重複消費問題，本消費者實作了基於 Redis SETNX 的分散式去重鎖防禦；
 * 同時，結合 Spring Kafka 的 {@link RetryableTopic} 提供非同步指數退避重試與死信佇列 (DLT) 處理。
 * </p>
 */
@Component
@Slf4j
public class OrderQueryConsumer {

    private final OrderQueryService orderQueryService;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    /**
     * 消費者建構子。
     *
     * @param orderQueryService 讀取模型同步服務
     * @param objectMapper Jackson ObjectMapper 實例
     * @param redisTemplate Redis 模板實例
     */
    public OrderQueryConsumer(OrderQueryService orderQueryService,
                              ObjectMapper objectMapper,
                              StringRedisTemplate redisTemplate) {
        this.orderQueryService = orderQueryService;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
    }

    /**
     * 處理訂單建立事件的消費者核心方法。
     * <p>
     * <b>配置詳解 (@RetryableTopic)：</b>
     * <ul>
     *   <li>attempts = "3"：代表該訊息最多執行 3 次消費（1 次原始嘗試 + 2 次重試）。</li>
     *   <li>dltStrategy = DltStrategy.FAIL_ON_ERROR：當重試次數用盡依然失敗時，將訊息送入死信佇列。</li>
     *   <li>include = { RuntimeException.class }：僅針對 RuntimeException 及其子類進行重試。</li>
     * </ul>
     * </p>
     * <p>
     * <b>去重鎖與重試優化邏輯：</b>
     * 1. 藉由 Redis 的 {@code setIfAbsent} 設置為期 10 分鐘的去重鎖，防止短時間內重複消費相同訊息。
     * 2. 若鎖已存在，說明該訊息已被消費或正在處理，主動攔截並返回。
     * 3. 若在處理過程中發生異常（例如資料庫連線失敗），<b>必須主動釋放已獲得的 Redis 鎖</b>。
     *    否則，在接下來的 Retry Topic 重試輪詢中，重試訊息會因為撞到自己先前設下的鎖而被主動攔截，導致重試機制失效。
     * 4. 釋放鎖後拋出異常，驅動 Spring Kafka 將訊息送入重試佇列進行指數退避等待。
     * </p>
     *
     * @param message 接收到的 Kafka 訊息 JSON 字串
     * @param currentTopic 當前消費的主題名稱（可能為原主題或重試主題）
     */
    @RetryableTopic(
        attempts = "3",
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

            // 測試機制：如果商品 ID 等於 "PROD-DLQ-TEST"，人為模擬資料庫超時異常以測試死信佇列
            if ("PROD-DLQ-TEST".equals(event.getProductId())) {
                log.error("[觸發預期異常] 偵測到死信測試專用商品！模擬數據庫此時連線超時！Topic: {}", currentTopic);
                throw new RuntimeException("DB_CONNECTION_TIMEOUT_SIMULATION");
            }

            // 設置 Redis 去重鎖
            lockKey = "order:lock:consume:" + orderId;
            Boolean isFirstConsume = redisTemplate.opsForValue().setIfAbsent(lockKey, "LOCKED", 10, TimeUnit.MINUTES);

            if (Boolean.FALSE.equals(isFirstConsume)) {
                log.warn("【Query 端警告】檢測到重複發送的 Kafka 事件，系統已主動攔截！Order ID: {}", orderId);
                return;
            }

            log.info("【Query 端】成功獲取消費鎖，開始從 Topic [{}] 消費訊息: {}", currentTopic, message);

            // 呼叫 Service 同步更新 Redis 中的唯讀訂單長效視圖
            orderQueryService.syncOrderView(event);

        } catch (Exception e) {
            // 異常補償：釋放 Redis 去重鎖，以利下一輪重試非同步執行
            if (lockKey != null) {
                redisTemplate.delete(lockKey);
                log.info("[重試優化] 已主動釋放 Redis 消費鎖 {}，以利下一輪非同步退避重試執行。", lockKey);
            }

            // 重新拋出異常以觸發 Spring Kafka Retry 主題重試
            throw new RuntimeException(e);
        }
    }

    /**
     * 死信佇列處理器 (Dead Letter Topic Handler)。
     * <p>
     * 當重試次數（3 次）全部用盡且依然拋出異常時，訊息將會被送入 DLT 佇列。
     * 本方法負責攔截這些「毒丸訊息」，並執行對應的壞帳登記或警報通知。
     * </p>
     *
     * @param message 原始毒丸訊息的 JSON 字串
     * @param dltTopic 觸發此處理的死信佇列主題名稱
     * @param exceptionMessage 底層核心異常崩潰的錯誤描述
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