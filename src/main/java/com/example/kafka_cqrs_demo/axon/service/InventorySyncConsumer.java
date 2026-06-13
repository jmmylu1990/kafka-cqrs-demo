package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.config.InventoryKafkaConfig;
import com.example.kafka_cqrs_demo.axon.dto.InventorySyncEvent;
import com.example.kafka_cqrs_demo.axon.repository.AxonDlqMessageRepository;
import com.example.kafka_cqrs_demo.entity.AxonDlqMessageEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * 庫存非同步同步消費者 (Inventory Sync Consumer)
 * <p>
 * 負責監聽 Kafka 中的 {@code inventory-sync-events} 主題。
 * 重構後使用策略模式 (Strategy Pattern) 進行事件派發，消除冗長的 switch-case 並抽離資料庫依賴，符合 OCP。
 * </p>
 */
@Slf4j
@Service
public class InventorySyncConsumer {

    private final AxonDlqMessageRepository dlqMessageRepository;
    private final ObjectMapper objectMapper;
    private final Map<String, InventorySyncHandler> handlerMap;

    public InventorySyncConsumer(AxonDlqMessageRepository dlqMessageRepository,
                                 ObjectMapper objectMapper,
                                 Map<String, InventorySyncHandler> handlerMap) {
        this.dlqMessageRepository = dlqMessageRepository;
        this.objectMapper = objectMapper;
        this.handlerMap = handlerMap;
    }

    /**
     * 監聽並處理庫存同步 Kafka 訊息。
     *
     * @param message Kafka 收到的 JSON 字串訊息
     */
    @KafkaListener(topics = InventoryKafkaConfig.INVENTORY_SYNC_TOPIC, groupId = "inventory-sync-group")
    @Transactional
    public void consume(String message) throws Exception {
        log.info("[InventorySyncConsumer] 收到庫存同步 Kafka 訊息: {}", message);

        // 不在此處捕捉 DB/業務異常，讓異常向上拋出以觸發 Spring Kafka 重試與 DLQ 流程
        InventorySyncEvent event = objectMapper.readValue(message, InventorySyncEvent.class);

        log.info("[InventorySyncConsumer] 解析事件成功: orderId={}, action={}", event.getOrderId(), event.getActionType());

        // 依據 actionType 從策略 Map 中尋找對應的 Handler 進行業務處理
        InventorySyncHandler handler = handlerMap.get(event.getActionType());
        if (handler != null) {
            handler.handle(event);
        } else {
            log.warn("[InventorySyncConsumer] 未知的同步動作類別: {}", event.getActionType());
        }
    }

    /**
     * 監聽死信佇列 (DLQ)，模擬發送警報簡訊並持久化記錄於資料庫中以支援一鍵重試。
     *
     * @param message          進入死信佇列的原始訊息內容
     * @param exceptionMessage 失敗例外原因說明
     * @param originalTopic    原始訊息發送的主題
     */
    @KafkaListener(topics = InventoryKafkaConfig.INVENTORY_SYNC_DLQ_TOPIC, groupId = "inventory-dlq-group")
    @Transactional
    public void consumeDlq(String message,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic) {
        log.error("[ALERT] [SMS GATEWAY] 警報：庫存同步訊息進入死信佇列 (DLQ)！已發送簡訊通知工程師人工排查。訊息內容: {}, 錯誤原因: {}", message,
                exceptionMessage);

        // 將死信訊息寫入 MySQL 以供重試與審計
        AxonDlqMessageEntity dlqMessage = new AxonDlqMessageEntity(
                UUID.randomUUID().toString(),
                message,
                originalTopic != null ? originalTopic : InventoryKafkaConfig.INVENTORY_SYNC_TOPIC,
                exceptionMessage != null
                        ? (exceptionMessage.length() > 500 ? exceptionMessage.substring(0, 500) : exceptionMessage)
                        : "未知處理異常",
                "PENDING",
                LocalDateTime.now());
        dlqMessageRepository.save(dlqMessage);
    }
}
