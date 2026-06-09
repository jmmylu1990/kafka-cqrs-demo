package com.example.kafka_cqrs_demo.axon.repository;

import com.example.kafka_cqrs_demo.entity.AxonWalletTransactionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Axon 錢包交易流水 JPA 數據訪問接口 (Axon Wallet Transaction Repository)
 */
@Repository
public interface AxonWalletTransactionRepository extends JpaRepository<AxonWalletTransactionEntity, String> {
    Optional<AxonWalletTransactionEntity> findByOrderIdAndType(String orderId, String type);
}
