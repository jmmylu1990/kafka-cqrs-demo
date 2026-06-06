package com.example.kafka_cqrs_demo.axon.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JacksonConfig {

    @Bean
    @Primary // 🟢 宣告這是全域最高優先權的 ObjectMapper，Spring Boot 與 Axon 將統一聽它的指揮
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();

        // 🎯 自動註冊 Java 8 時間模組（如 LocalDateTime），防止以後時間序列化噴錯
        objectMapper.registerModule(new JavaTimeModule());

        return objectMapper;
    }
}