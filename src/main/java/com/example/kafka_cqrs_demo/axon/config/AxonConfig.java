package com.example.kafka_cqrs_demo.axon.config;

import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.Configuration;
import org.axonframework.config.ConfigurationScopeAwareProvider;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.SimpleDeadlineManager;
import org.springframework.context.annotation.Bean;

/**
 * Axon 框架專用配置類別 (Axon Configuration)
 * <p>
 * 用於自訂 Axon Framework 的核心基礎組件。本專案目前於此類別中配置了 {@link DeadlineManager}，
 * 以便能夠在 Saga 流程（例如訂單付款逾時）中，排程與處理具備時間期限的業務邏輯任務。
 * </p>
 */
@org.springframework.context.annotation.Configuration
public class AxonConfig {

    /**
     * 配置系統內建的逾時管理器 SimpleDeadlineManager。
     * <p>
     * 該管理器允許 Saga 在執行中安排一個在特定時間後觸發的任務。
     * 為了使其具備 Aggregate 與 Saga 的感知能力，我們傳入了 {@link ConfigurationScopeAwareProvider}；
     * 同時，為了確保逾時動作的資料一致性，此處注入並配置了 Spring 的 {@link TransactionManager}，
     * 讓所有的逾時補償指令（例如自動取消）均在事務邊界內安全執行。
     * </p>
     *
     * @param configuration Axon 框架的全域 Configuration 實例，用以提供範疇感知支援
     * @param transactionManager Axon 的事務管理器，用以確保逾時呼叫的事務性
     * @return 建立完成的 SimpleDeadlineManager 實例
     */
    @Bean
    public DeadlineManager deadlineManager(Configuration configuration, TransactionManager transactionManager) {
        return SimpleDeadlineManager.builder()
                .scopeAwareProvider(new ConfigurationScopeAwareProvider(configuration))
                .transactionManager(transactionManager)
                .build();
    }
}
