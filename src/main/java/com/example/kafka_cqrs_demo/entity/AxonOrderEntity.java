package com.example.kafka_cqrs_demo.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.LocalDateTime;
@Entity
@Table(name = "t_axon_order")
@Data
public class AxonOrderEntity {
    @Id
    private String orderId;
    private String productId; 
    private int quantity;
    private long price;
    private String status;
    private String cancelReason;
    private LocalDateTime createTime;
}
