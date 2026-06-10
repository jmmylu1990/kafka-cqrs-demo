package com.example.kafka_cqrs_demo.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 金流退款重試任務實體 (Axon Refund Retry Task Entity)
 * <p>
 * 當 SAGA 補償交易中外部退款 HTTP 呼叫失敗時，將此任務持久化於 MySQL 中。
 * 背景自癒排程器將定期掃描並採用指數退避策略進行重試。
 * </p>
 */
@Entity
@Table(name = "t_payment_refund_retry_task")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AxonRefundRetryTaskEntity {

    /** 任務唯一識別碼 (UUID) */
    @Id
    @Column(length = 50)
    private String id;

    /** 使用者識別碼 */
    @Column(length = 50, nullable = false)
    private String userId;

    /** 訂單識別碼 (唯一，用於保障等冪性防止重複建立) */
    @Column(length = 50, nullable = false, unique = true)
    private String orderId;

    /** 退款金額 */
    @Column(nullable = false)
    private long amount;

    /** 當前已嘗試重試次數 */
    @Column(nullable = false)
    private int retryCount;

    /** 任務處理狀態：PENDING (待重試), SUCCESS (已成功完成), FAILED (超過次數上限永久失敗) */
    @Column(length = 20, nullable = false)
    private String status;

    /** 最後一次退款失敗的錯誤例外說明 */
    @Column(length = 500)
    private String lastErrorMessage;

    /** 下一次允許重試的執行時間點 */
    @Column(nullable = false)
    private LocalDateTime nextRetryTime;

    /** 任務建立時間 */
    @Column(nullable = false)
    private LocalDateTime createTime;

    /** 任務最後更新時間 */
    @Column(nullable = false)
    private LocalDateTime updateTime;
}
