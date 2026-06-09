package com.example.kafka_cqrs_demo.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * 庫存預留記錄實體 (Axon Stock Reservation Entity)
 * <p>
 * 用於追蹤特定訂單預留庫存的狀態（RESERVED: 已預留，COMPLETED: 已確認扣減/訂單付款，RELEASED: 已釋放/訂單取消）。
 * 這有助於在訂單取消事件 (OrderCancelledEvent) 發生時，能夠根據 orderId 查得對應的 productId 與 quantity，
 * 以便安全且正確地進行庫存退回 (Compensating action)。
 * </p>
 */
@Entity
@Table(name = "t_axon_stock_reservation")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AxonStockReservationEntity {

    /** 訂單唯一識別碼，主鍵 (每個訂單目前對應一筆預留紀錄) */
    @Id
    private String orderId;

    /** 被預留的產品識別碼 */
    private String productId;

    /** 預留商品數量 */
    private int quantity;

    /** 預留狀態 (RESERVED, COMPLETED, RELEASED) */
    private String status;

    /** 狀態最近更新時間 */
    private LocalDateTime updateTime;
}
