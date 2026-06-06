package com.example.kafka_cqrs_demo.axon.command;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * 取消訂單指令 (Cancel Order Command)
 * <p>
 * 當發生庫存不足、付款逾時或使用者主動申請取消時，發送此指令以驅動訂單聚合根變更狀態為 CANCELLED。
 * </p>
 */
@Value
public class CancelOrderCommand {

    /**
     * 目標聚合識別碼
     * 指明此取消指令要作用在對應的訂單聚合根實例上。
     */
    @TargetAggregateIdentifier
    private String orderId;

    /** 取消訂單的詳細原因說明 */
    private String reason;
}
