package com.example.kafka_cqrs_demo.axon.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * 庫存同步 Kafka 主題配置類別 (Inventory Kafka Configuration)
 * <p>
 * 在系統啟動時自動向 Kafka Broker 註冊用於同步 Redis 與 MySQL 庫存狀態的主題。
 * </p>
 */
@Configuration
public class InventoryKafkaConfig {

    /** 庫存非同步同步的 Kafka Topic 名稱 */
    public static final String INVENTORY_SYNC_TOPIC = "inventory-sync-events";

    /** 庫存同步失敗的死信佇列 (DLQ) Kafka Topic 名稱 */
    public static final String INVENTORY_SYNC_DLQ_TOPIC = "inventory-sync-events.DLQ";

    /**
     * 定義並配置庫存同步事件的 Kafka Topic。
     *
     * @return NewTopic 配置實例
     */
    @Bean
    public NewTopic inventorySyncTopic() {
        return TopicBuilder.name(INVENTORY_SYNC_TOPIC)
                .partitions(3)          // 設為 3 個分區以配合高併發
                .replicas(1)            // 本地開發複製因子為 1
                .build();
    }

    /**
     * 定義並配置死信佇列 (DLQ) 的 Kafka Topic。
     *
     * @return NewTopic 配置實例
     */
    @Bean
    public NewTopic inventorySyncDlqTopic() {
        return TopicBuilder.name(INVENTORY_SYNC_DLQ_TOPIC)
                .partitions(1)          // 死信佇列設為 1 個分區即可
                .replicas(1)            // 本地開發複製因子為 1
                .build();
    }

    /**
     * 配置 Spring Kafka 的通用異常處理器 (CommonErrorHandler)。
     * <p>
     * 當 Consumer 拋出異常時：
     * 1. 針對可重試異常，自動進行 3 次重試，每次間隔 1 秒。
     * 2. 若 3 次重試後依然失敗，自動將訊息發送到死信佇列 (DLQ)。
     * 3. 針對 Jackson JSON 反序列化等非重試異常 (JsonProcessingException)，直接進入 DLQ，防止阻礙佇列。
     * </p>
     *
     * @param kafkaTemplate 用於轉發死信訊息的 Kafka 範本
     * @return 異常處理器實例
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        // 建立死信轉發器，指定目標 Topic 爲 INVENTORY_SYNC_DLQ_TOPIC，並發送到 partition 0 (避免因 DLQ 分區數較少而發生 non-existent partition 警告)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(INVENTORY_SYNC_DLQ_TOPIC, 0));
        
        // 設定重試策略：最大重試 3 次，每次間隔 1000 毫秒
        FixedBackOff backOff = new FixedBackOff(1000L, 3L);
        
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        
        // 分類排除：反序列化失敗 (毒藥丸) 直接送 DLQ，不進行重試
        errorHandler.addNotRetryableExceptions(com.fasterxml.jackson.core.JsonProcessingException.class);
        
        return errorHandler;
    }
}
