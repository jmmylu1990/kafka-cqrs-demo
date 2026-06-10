package com.example.kafka_cqrs_demo.axon.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

/**
 * 重試機制配置類別 (Retry Configuration)
 * <p>
 * 藉由 {@code @EnableRetry} 啟用 Spring Retry 聲明式重試架構。
 * 讓金流防腐層呼叫外部 API 時，能透過 {@code @Retryable} 自動處理網路瞬斷與超時重試。
 * </p>
 */
@Configuration
@EnableRetry
public class RetryConfig {
}
