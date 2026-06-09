package com.example.kafka_cqrs_demo.axon.repository;

import com.example.kafka_cqrs_demo.entity.AxonOrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Axon 訂單 JPA 數據訪問接口 (Axon Order Repository)
 */
@Repository
public interface AxonOrderRepository extends JpaRepository<AxonOrderEntity, String> {
}
