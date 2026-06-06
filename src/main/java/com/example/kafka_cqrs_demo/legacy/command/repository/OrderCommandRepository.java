package com.example.kafka_cqrs_demo.legacy.command.repository;

import com.example.kafka_cqrs_demo.legacy.command.domain.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 傳統模式寫入端訂單資料存取介面 (Legacy Order Command Repository)
 * <p>
 * 繼承 {@link JpaRepository}，用以對 MySQL 中的訂單實體進行持久化存取操作（新增、修改與查詢）。
 * </p>
 */
@Repository
public interface OrderCommandRepository extends JpaRepository<OrderEntity, String> {
}
