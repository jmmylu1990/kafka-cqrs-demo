package com.example.kafka_cqrs_demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.ComponentScan;

/**
 * Spring Boot 應用程式入口點 (Spring Boot Entry Point)
 * <p>
 * 啟動本 CQRS 與 Axon Saga 範例專案。
 * 本類別藉由標註 {@link SpringBootApplication} 與 {@link ComponentScan}，
 * 自動配置 Spring 應用程式環境、掃描並載入專案下所有的 Bean、控制器、服務及 Axon 配置。
 * </p>
 */
@SpringBootApplication
@ComponentScan(basePackages = "com.example.kafka_cqrs_demo")
@EntityScan(basePackages = {
        "com.example.kafka_cqrs_demo",
        "org.axonframework.eventsourcing.eventstore.jpa",
        "org.axonframework.modelling.saga.repository.jpa",
        "org.axonframework.eventhandling.tokenstore.jpa",
        "org.axonframework.eventhandling.deadletter.jpa"
})
public class KafkaCqrsDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaCqrsDemoApplication.class, args);
    }
}
