package com.example.kafka_cqrs_demo.axon.repository;

import com.example.kafka_cqrs_demo.entity.AxonRefundRetryTaskEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * 退款重試任務資料庫存取接口 (Axon Refund Retry Task Repository)
 * <p>
 * 提供退款重試任務的持久化查詢功能，主要用於背景排程掃描與等冪查重。
 * </p>
 */
@Repository
public interface AxonRefundRetryTaskRepository extends JpaRepository<AxonRefundRetryTaskEntity, String> {

    /**
     * 查詢指定狀態且下一次重試時間已到期之退款任務列表。
     *
     * @param status 任務狀態 (例如 PENDING)
     * @param time 目前系統時間
     * @return 符合到期重試條件的任務列表
     */
    List<AxonRefundRetryTaskEntity> findByStatusAndNextRetryTimeBefore(String status, LocalDateTime time);

    /**
     * 依據訂單識別碼查詢退款重試任務（用於等冪性查重）。
     *
     * @param orderId 訂單唯一識別碼
     * @return 退款重試任務實體封裝
     */
    Optional<AxonRefundRetryTaskEntity> findByOrderId(String orderId);
}
