package com.example.kafka_cqrs_demo.repository;

import com.example.kafka_cqrs_demo.entity.OutboxEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 發件箱資料存取介面 (Outbox Repository)
 * <p>
 * 提供對 t_outbox 資料表的資料庫存取操作。
 * </p>
 */
@Repository
public interface OutboxRepository extends JpaRepository<OutboxEntity, String> {

    /**
     * 獲取指定狀態的發件箱紀錄，並依照建立時間升序排列，保證同分區訊息的發送順序性。
     *
     * @param status 狀態 (例如 PENDING)
     * @return 待處理的 Outbox 列表 (最多 50 筆)
     */
    List<OutboxEntity> findTop50ByStatusOrderByCreateTimeAsc(String status);

    /**
     * 計算指定狀態的發件箱紀錄數量，用於 Prometheus 監控指標。
     *
     * @param status 狀態 (例如 PENDING)
     * @return 數量
     */
    long countByStatus(String status);
}
