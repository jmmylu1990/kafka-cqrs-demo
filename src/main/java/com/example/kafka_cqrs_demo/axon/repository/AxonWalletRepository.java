package com.example.kafka_cqrs_demo.axon.repository;

import com.example.kafka_cqrs_demo.entity.AxonWalletEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Axon 錢包 JPA 數據訪問接口 (Axon Wallet Repository)
 */
@Repository
public interface AxonWalletRepository extends JpaRepository<AxonWalletEntity, String> {
}
