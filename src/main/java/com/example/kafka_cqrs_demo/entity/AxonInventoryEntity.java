package com.example.kafka_cqrs_demo.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * Axon 模式下的庫存 JPA 實體 (Axon Inventory Entity)
 * <p>
 * 對應資料庫中的 t_axon_inventory 資料表，用以維護各商品的可用庫存與預留庫存。
 * </p>
 */
@Entity
@Table(name = "t_axon_inventory")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AxonInventoryEntity {

    /** 產品唯一識別碼 (Primary Key) */
    @Id
    private String productId;

    /** 可用庫存數量 */
    private int stock;

    /** 當前已被 Saga 流程預留/鎖定但尚未完成交易的庫存數量 */
    private int reservedStock;

    /** 版本號，用以支援 JPA 樂觀鎖 */
    @Version
    private Long version;

    /**
     * 自定義三參數建構子，維持與資料庫初始化器與單元測試之相容性。
     */
    public AxonInventoryEntity(String productId, int stock, int reservedStock) {
        this.productId = productId;
        this.stock = stock;
        this.reservedStock = reservedStock;
    }
}
