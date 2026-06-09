package com.example.kafka_cqrs_demo.axon.repository;

import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Axon 庫存 JPA 數據訪問接口 (Axon Inventory Repository)
 */
@Repository
public interface AxonInventoryRepository extends JpaRepository<AxonInventoryEntity, String> {
}
