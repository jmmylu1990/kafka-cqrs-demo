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
 * Axon 模式下的死信佇列 (DLQ) 訊息實體 (Axon DLQ Message Entity)
 * <p>
 * 對應資料庫中的 t_axon_dlq_message 資料表，用以持久化存儲消費失敗的 Kafka 訊息。
 * </p>
 */
@Entity
@Table(name = "t_axon_dlq_message")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AxonDlqMessageEntity {

    /** 訊息唯一識別碼 (UUID) */
    @Id
    @Column(length = 50)
    private String id;

    /** 原始 Kafka 訊息 JSON 內容 */
    @Column(columnDefinition = "TEXT", nullable = false)
    private String messageContent;

    /** 訊息來源的原始 Kafka 主題 (Topic) */
    @Column(length = 100, nullable = false)
    private String topic;

    /** 造成訊息處理失敗的例外原因說明 */
    @Column(length = 500)
    private String errorMessage;

    /** 處理狀態：PENDING (待重試), REPROCESSED (已重新處理), FAILED (處理失敗) */
    @Column(length = 20, nullable = false)
    private String status;

    /** 訊息進入死信資料表的建立時間 */
    @Column(nullable = false)
    private LocalDateTime createTime;
}
