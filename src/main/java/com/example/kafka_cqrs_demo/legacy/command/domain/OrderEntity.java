package com.example.kafka_cqrs_demo.legacy.command.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Table(name = "t_order")
public @Data class OrderEntity {
    @Id
    private String orderId; // 使用 UUID
    private String productId;
    private int quantity;
    private long price;
    private String status; // PENDING, SUCCESS, FAILED
    private LocalDateTime createTime;
}
