package com.example.kafka_cqrs_demo.legacy.command.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 傳統模式寫入端訂單 JPA 實體 (Legacy Order Entity)
 * <p>
 * 對應資料庫中的 t_order 資料表。用於傳統模式下保存訂單的最新狀態。
 * </p>
 */
@Entity
@Table(name = "t_order")
@Data
public class OrderEntity {

    /** 訂單唯一識別碼，主鍵 (UUID) */
    @Id
    private String orderId;

    /** 購買的產品識別碼 */
    private String productId;

    /** 購買的商品數量 */
    private int quantity;

    /** 商品單價 */
    private long price;

    /** 訂單當前狀態（例如：PENDING, SUCCESS, FAILED） */
    private String status;

    /** 訂單的建立時間 */
    private LocalDateTime createTime;
}
