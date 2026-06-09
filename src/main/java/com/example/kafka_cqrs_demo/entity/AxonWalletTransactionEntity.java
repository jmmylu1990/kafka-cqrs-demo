package com.example.kafka_cqrs_demo.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.time.LocalDateTime;

/**
 * Axon 模式下的錢包交易明細 JPA 實體 (Axon Wallet Transaction Entity)
 * <p>
 * 對應資料庫中的 t_axon_wallet_transaction 資料表，用於記錄每一次的扣款與退款明細。
 * </p>
 */
@Entity
@Table(name = "t_axon_wallet_transaction")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AxonWalletTransactionEntity {

    /** 交易流水唯一識別碼 (Primary Key) */
    @Id
    private String transactionId;

    /** 關聯的使用者識別碼 */
    private String userId;

    /** 關聯的訂單識別碼 */
    private String orderId;

    /** 異動金額 (正數代表充值/退款，負數代表扣款) */
    private long amount;

    /** 交易類型 (DEBIT: 扣款, REFUND: 退款) */
    private String type;

    /** 交易狀態 (SUCCESS: 成功, FAILED: 失敗) */
    private String status;

    /** 記錄更新時間 */
    private LocalDateTime updateTime;
}
