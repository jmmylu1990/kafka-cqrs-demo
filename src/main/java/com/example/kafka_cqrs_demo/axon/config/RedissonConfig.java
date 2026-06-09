package com.example.kafka_cqrs_demo.axon.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson 分散式鎖與客戶端配置類別 (Redisson Configuration)
 * <p>
 * 定義 RedissonClient Bean，連接至本地 Redis 服務。
 * 手動配置以避免與 Spring Boot Data Redis (Lettuce) 的自動配置衝突。
 * </p>
 */
@Configuration
public class RedissonConfig {

    /**
     * 註冊 RedissonClient 實例。
     *
     * @return RedissonClient 實例
     */
    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        // 連接至本地 Redis 預設埠口 6379
        config.useSingleServer()
                .setAddress("redis://127.0.0.1:6379")
                .setConnectionMinimumIdleSize(5)
                .setConnectionPoolSize(10);
        return Redisson.create(config);
    }
}
