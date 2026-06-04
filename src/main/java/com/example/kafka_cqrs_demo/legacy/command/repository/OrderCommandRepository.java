package com.example.kafka_cqrs_demo.legacy.command.repository;

import com.example.kafka_cqrs_demo.legacy.command.domain.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderCommandRepository extends JpaRepository<OrderEntity, String> {
}
