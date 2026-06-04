package com.example.kafka_cqrs_demo.legacy.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    public static final String TOPIC_NAME = "order-events";

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(TOPIC_NAME)
            .partitions(3)          // 設定 3 個分區，用來承載高併發流量
            .replicas(1)            // 複製因子（因為本機是單機版 Kafka，所以設 1；實務叢集會設 3）
            .build();
    }
}
