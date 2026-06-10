package com.example.kafka_cqrs_demo.axon.repository;

import com.example.kafka_cqrs_demo.entity.AxonDlqMessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 死信訊息 (DLQ) 倉儲介面 (Axon DLQ Message Repository)
 * <p>
 * 提供對 t_axon_dlq_message 資料表的常規 CRUD 操作。
 * </p>
 */
@Repository
public interface AxonDlqMessageRepository extends JpaRepository<AxonDlqMessageEntity, String> {

    /**
     * 根據處理狀態查詢死信訊息列表。
     *
     * @param status 狀態類型 (如 PENDING)
     * @return 匹配的死信訊息列表
     */
    List<AxonDlqMessageEntity> findByStatus(String status);
}
