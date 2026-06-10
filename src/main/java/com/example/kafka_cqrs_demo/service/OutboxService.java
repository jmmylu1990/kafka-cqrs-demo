package com.example.kafka_cqrs_demo.service;

import com.example.kafka_cqrs_demo.entity.OutboxEntity;
import com.example.kafka_cqrs_demo.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 發件箱服務層 (Outbox Service)
 * <p>
 * 提供 Outbox 訊息的存檔、非同步即時發送、重試失敗記錄與狀態更新。
 * </p>
 */
@Service
@Slf4j
public class OutboxService {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 延遲注入自我代理，用於避免內部調用導致 @Transactional(propagation = Propagation.REQUIRES_NEW) 失效。
     */
    @Lazy
    @Autowired
    private OutboxService self;

    public OutboxService(OutboxRepository outboxRepository,
                         KafkaTemplate<String, String> kafkaTemplate,
                         ObjectMapper objectMapper,
                         MeterRegistry meterRegistry) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;

        // 註冊 Prometheus 自訂 Gauge 指標，監控發件箱積壓量
        Gauge.builder("outbox_pending_count", outboxRepository, repo -> repo.countByStatus("PENDING"))
                .description("Number of pending messages in the Outbox table")
                .register(meterRegistry);
    }

    /**
     * 將訊息包裹於目前的本地數據庫交易中存檔，並註冊交易提交後的即時發送任務。
     *
     * @param topic   Kafka Topic
     * @param key     Kafka Key (例如 orderId / productId)
     * @param payload 訊息的實體物件
     */
    @Transactional
    public void saveEvent(String topic, String key, Object payload) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(payload);
            OutboxEntity entity = new OutboxEntity(
                    UUID.randomUUID().toString(),
                    topic,
                    key,
                    jsonPayload,
                    "PENDING",
                    0,
                    null,
                    LocalDateTime.now(),
                    LocalDateTime.now()
            );

            // 1. 本地事務內存檔
            outboxRepository.save(entity);
            log.info("[OutboxService] 訊息已保存至 t_outbox。ID: {}, Topic={}, Key={}", entity.getId(), topic, key);

            // 2. 註冊交易提交後的 Hook，以降低發送延遲
            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        log.info("[OutboxService] 交易提交成功，立刻觸發非同步發送任務。ID: {}", entity.getId());
                        CompletableFuture.runAsync(() -> self.publishEvent(entity));
                    }
                });
            } else {
                // 若不在 Spring 事務中，直接非同步發送
                log.info("[OutboxService] 當前無活躍交易，直接觸發非同步發送。ID: {}", entity.getId());
                CompletableFuture.runAsync(() -> self.publishEvent(entity));
            }

        } catch (Exception e) {
            log.error("[OutboxService] 儲存發件箱訊息失敗: {}", e.getMessage(), e);
            throw new RuntimeException("寫入發件箱失敗，交易回滾", e);
        }
    }

    /**
     * 執行將訊息發送至 Kafka 的任務。
     *
     * @param entity 發件箱記錄
     */
    public void publishEvent(OutboxEntity entity) {
        log.info("[OutboxService] 嘗試發送 Outbox 訊息: Topic={}, Key={}, ID={}",
                entity.getTopic(), entity.getMessageKey(), entity.getId());

        // 模擬特定測試商品 (PROD-OUTBOX-FAIL) 的發送異常，用於手動測試 outbox 補償自癒與重試
//        if ("PROD-OUTBOX-FAIL".equals(entity.getMessageKey()) ||
//                (entity.getPayload() != null && entity.getPayload().contains("PROD-OUTBOX-FAIL"))) {
//            log.warn("[OutboxService] [TEST MODE] 偵測到測試 Key/Payload，故意模擬 Kafka 發送連線異常！");
//            self.handleFailedSend(entity.getId(), "模擬 Kafka 連線超時異常 (測試商品 PROD-OUTBOX-FAIL)");
//            return;
//        }

        try {
            // 同步等待發送結果以確保正確取得送出異常
            kafkaTemplate.send(entity.getTopic(), entity.getMessageKey(), entity.getPayload())
                    .get(3, TimeUnit.SECONDS);

            // 發送成功，標記為已處理
            self.markAsProcessed(entity.getId());
        } catch (Exception e) {
            log.error("[OutboxService] 訊息發送至 Kafka 失敗。ID: {}, 原因: {}", entity.getId(), e.getMessage());
            self.handleFailedSend(entity.getId(), e.getMessage());
        }
    }

    /**
     * 在獨立的事務中更新 Outbox 狀態為已發送完成。
     *
     * @param id Outbox ID
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markAsProcessed(String id) {
        outboxRepository.findById(id).ifPresent(entity -> {
            entity.setStatus("PROCESSED");
            entity.setUpdateTime(LocalDateTime.now());
            outboxRepository.save(entity);
            log.info("[OutboxService] 成功更新 Outbox 狀態為 PROCESSED。ID: {}", id);
        });
    }

    /**
     * 在獨立的事務中累加重試次數，並於達到重試上限 (5 次) 時標記為 FAILED。
     *
     * @param id           Outbox ID
     * @param errorMessage 錯誤訊息
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void handleFailedSend(String id, String errorMessage) {
        outboxRepository.findById(id).ifPresent(entity -> {
            int newRetry = entity.getRetryCount() + 1;
            entity.setRetryCount(newRetry);
            entity.setErrorMessage(errorMessage != null && errorMessage.length() > 500
                    ? errorMessage.substring(0, 500)
                    : errorMessage);

            if (newRetry >= 5) {
                entity.setStatus("FAILED");
                log.error("[OutboxService] Outbox 訊息重試達上限，標記為 FAILED！ID: {}, 錯誤: {}", id, errorMessage);
            } else {
                entity.setStatus("PENDING");
                log.warn("[OutboxService] Outbox 訊息發送失敗，保留 PENDING。當前重試次數: {}/5. ID: {}, 錯誤: {}",
                        newRetry, id, errorMessage);
            }

            entity.setUpdateTime(LocalDateTime.now());
            outboxRepository.save(entity);
        });
    }
}
