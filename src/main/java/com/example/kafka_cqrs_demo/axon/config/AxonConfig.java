package com.example.kafka_cqrs_demo.axon.config;

import jakarta.persistence.EntityManager;
import javax.sql.DataSource;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.jpa.SimpleEntityManagerProvider;
import org.axonframework.spring.messaging.unitofwork.SpringTransactionManager;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.Configuration;
import org.axonframework.config.ConfigurationScopeAwareProvider;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.SimpleDeadlineManager;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.jpa.JpaEventStorageEngine;
import org.axonframework.serialization.Serializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.jpa.JpaTokenStore;

/**
 * Axon 框架專用配置類別 (Axon Configuration)
 * <p>
 * 用於自訂 Axon Framework 的核心基礎組件。
 * 包含 DeadlineManager 用於超時流程，以及顯式註冊 JPA EventStorageEngine 以免在 Spring Boot 4 環境下找不到 EventStore。
 * 同時設置全域的 Spring 事務管理器，保證指令處理與事件存儲在資料庫事務內執行。
 * </p>
 */
@org.springframework.context.annotation.Configuration
public class AxonConfig {

    /**
     * 配置系統內建的逾時管理器 SimpleDeadlineManager。
     */
    @Bean
    public DeadlineManager deadlineManager(Configuration configuration, TransactionManager transactionManager) {
        return SimpleDeadlineManager.builder()
                .scopeAwareProvider(new ConfigurationScopeAwareProvider(configuration))
                .transactionManager(transactionManager)
                .build();
    }

    /**
     * 提供 JPA 的 EntityManagerProvider，讓 Axon EventStore 可以訪問 JPA 實體管理器。
     */
    @Bean
    public EntityManagerProvider entityManagerProvider(EntityManager entityManager) {
        return new SimpleEntityManagerProvider(entityManager);
    }

    /**
     * 顯式配置 JPA 核心的事件存儲引擎 JpaEventStorageEngine，
     * 以便將 Axon 事件溯源 (Event Sourcing) 事件保存到本地 MySQL 數據庫中。
     */
    @Bean
    public EventStorageEngine eventStorageEngine(
            @Qualifier("eventSerializer") Serializer eventSerializer,
            @Qualifier("serializer") Serializer snapshotSerializer,
            EntityManagerProvider entityManagerProvider,
            TransactionManager axonTransactionManager,
            DataSource dataSource) throws java.sql.SQLException {
        return JpaEventStorageEngine.builder()
                .eventSerializer(eventSerializer)
                .snapshotSerializer(snapshotSerializer)
                .entityManagerProvider(entityManagerProvider)
                .transactionManager(axonTransactionManager)
                .dataSource(dataSource)
                .build();
    }

    /**
     * 定義 Axon 事務管理器 (TransactionManager)，對接到 Spring 的 PlatformTransactionManager。
     * 確保 EventStore、CommandBus、Saga 和 EventProcessor 都在 Spring 交易範圍內執行。
     */
    @Bean
    public TransactionManager axonTransactionManager(PlatformTransactionManager platformTransactionManager) {
        return new SpringTransactionManager(platformTransactionManager);
    }

    /**
     * 註冊全域事務管理器至 Axon ConfigurerModule。
     * 解決 "No EntityManager with actual transaction available for current thread" 的問題。
     */
    @Bean
    public ConfigurerModule transactionManagerConfigurerModule(TransactionManager axonTransactionManager) {
        return configurer -> configurer.configureTransactionManager(
                c -> axonTransactionManager
        );
    }

    /**
     * 配置 JPA TokenStore 以便將事件處理器的進度（Token）持久化在資料庫中。
     * 防止應用程式重啟時重複重放所有歷史事件。
     */
    @Bean
    public TokenStore tokenStore(EntityManagerProvider entityManagerProvider, @Qualifier("serializer") Serializer serializer) {
        return JpaTokenStore.builder()
                .entityManagerProvider(entityManagerProvider)
                .serializer(serializer)
                .build();
    }
}
