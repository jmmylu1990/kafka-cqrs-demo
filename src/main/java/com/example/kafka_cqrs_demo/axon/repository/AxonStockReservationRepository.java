package com.example.kafka_cqrs_demo.axon.repository;

import com.example.kafka_cqrs_demo.entity.AxonStockReservationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Axon 庫存預留紀錄 JPA 數據訪問接口 (Axon Stock Reservation Repository)
 */
@Repository
public interface AxonStockReservationRepository extends JpaRepository<AxonStockReservationEntity, String> {
}
