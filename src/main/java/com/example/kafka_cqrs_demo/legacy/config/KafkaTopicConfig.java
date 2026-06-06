package com.example.kafka_cqrs_demo.legacy.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka 主題配置類別 (Kafka Topic Config)
 * <p>
 * 用於在 Spring Boot 啟動時，自動向 Kafka Broker 註冊所需要的 Topic 資源。
 * 這裡配置了傳統 Kafka 模式下所使用的訂單事件主題。
 * </p>
 */
@Configuration
public class KafkaTopicConfig {

    /** 訂單事件的 Kafka Topic 名稱 */
    public static final String TOPIC_NAME = "order-events";

    /**
     * 定義並配置訂單事件的 Kafka Topic。
     *
     * @return NewTopic 配置實例
     */
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(TOPIC_NAME)
            .partitions(3)          // 設定 3 個分區，用以提供高併發處理與負載平衡能力
            .replicas(1)            // 複製因子，本機開發環境設為 1，生產叢集環境建議配置 3
            .build();
    }
}
