package com.example.kafka_cqrs_demo.axon.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Jackson 序列化與反序列化配置類別
 * 用於定義全域及 Axon 框架統一使用的 ObjectMapper，以確保時間格式等進階型態的序列化一致性。
 */
@Configuration
public class JacksonConfig {

    /**
     * 定義全域主要的 ObjectMapper 實例。
     * 使用 @Primary 註解使其在 Spring Boot 依賴注入時具備最高優先權，
     * 讓 Spring MVC、Kafka 傳輸以及 Axon 事件儲存與序列化均統一使用此 ObjectMapper。
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();

        // 自動註冊 Java 8 時間模組（例如 Java 8 的 java.time.LocalDateTime 及其它 JSR-310 日期時間型態），
        // 以防止在處理日期時間序列化及反序列化時出現不支援型態的報錯。
        objectMapper.registerModule(new JavaTimeModule());

        return objectMapper;
    }
}