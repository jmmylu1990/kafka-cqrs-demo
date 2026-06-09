package com.example.kafka_cqrs_demo.axon.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

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
}
