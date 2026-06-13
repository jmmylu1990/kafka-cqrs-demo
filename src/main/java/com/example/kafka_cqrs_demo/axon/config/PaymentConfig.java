package com.example.kafka_cqrs_demo.axon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * 金流配置類別 (Payment Configuration)
 * <p>
 * 提供並註冊金流呼叫專用的 RestTemplate，設定超時防護以實現依賴注入與控制反轉。
 * </p>
 */
@Configuration
public class PaymentConfig {

    @Bean
    public RestTemplate paymentRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        // 配置具備 3 秒連接與讀取超時防護
        factory.setConnectTimeout(3000);
        factory.setReadTimeout(3000);
        return new RestTemplate(factory);
    }
}
