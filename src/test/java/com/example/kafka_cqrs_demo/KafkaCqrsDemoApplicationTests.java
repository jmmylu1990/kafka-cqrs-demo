package com.example.kafka_cqrs_demo;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 系統載入與整合測試 (Application Context Integration Test)
 * <p>
 * 使用 {@link SpringBootTest} 註解，在啟動測試時加載完整的 Spring Boot 應用程式上下文 (Application Context)。
 * 用以確保專案中的所有 Bean 相依關係、設定檔參數與第三方框架集成（如 Spring Kafka、Axon 等）均能正確載入並啟動。
 * </p>
 */
@SpringBootTest
class KafkaCqrsDemoApplicationTests {

    /**
     * 驗證 Spring 應用程式上下文是否能成功加載且無異常拋出。
     */
    @Test
    void contextLoads() {
        // 若上下文加載順利，此方法將正常結束，代表整合基礎配置無誤。
    }
}
