package com.example.kafka_cqrs_demo.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 發件箱實體 (Outbox Entity)
 * <p>
 * 用於實作 Outbox Pattern，保證本地資料庫事務寫入與 Kafka 訊息發送的最終一致性。
 * </p>
 */
@Entity
@Table(name = "t_outbox")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEntity {

    /** 唯一標記符 (UUID) */
    @Id
    private String id;

    /** 目標 Kafka Topic */
    @Column(nullable = false)
    private String topic;

    /** Kafka 訊息的 Key (例如訂單 ID)，用於分區順序性 */
    @Column(name = "message_key")
    private String messageKey;

    /** 訊息的 JSON 負載 */
    @Lob
    @Column(nullable = false, columnDefinition = "LONGTEXT")
    private String payload;

    /** 狀態：PENDING (待發送), PROCESSED (已發送完成), FAILED (失敗且達重試上限) */
    @Column(nullable = false)
    private String status;

    /** 重試次數 */
    @Column(name = "retry_count")
    private int retryCount;

    /** 錯誤訊息紀錄 */
    @Column(name = "error_message", length = 500)
    private String errorMessage;

    /** 建立時間 */
    @Column(nullable = false)
    private LocalDateTime createTime;

    /** 更新時間 */
    @Column(nullable = false)
    private LocalDateTime updateTime;
}
