package com.example.kafka_cqrs_demo.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

/**
 * Axon 模式下的錢包 JPA 實體 (Axon Wallet Entity)
 * <p>
 * 對應資料庫中的 t_axon_wallet 資料表，用以維護各使用者的錢包餘額。
 * </p>
 */
@Entity
@Table(name = "t_axon_wallet")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AxonWalletEntity {

    /** 使用者唯一識別碼 (Primary Key) */
    @Id
    private String userId;

    /** 錢包可用餘額，單位為貨幣最小單位（例如分） */
    private long balance;
}
