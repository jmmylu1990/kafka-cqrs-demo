package com.example.kafka_cqrs_demo.axon.command;

import org.axonframework.modelling.command.TargetAggregateIdentifier;
import lombok.Value;

/**
 * 預留庫存指令 (Reserve Stock Command)
 * <p>
 * 當訂單建立時，Saga 協調器會向庫存服務發送此指令，用以指示系統鎖定並扣減該訂單對應商品的庫存。
 * </p>
 */
@Value
public class ReserveStockCommand {

    /**
     * 目標聚合識別碼 (Target Aggregate Identifier)
     * 用以將此指令關聯並路由至對應的訂單聚合根實例。
     */
    @TargetAggregateIdentifier
    private String orderId;

    /** 欲預留商品庫存的產品唯一識別碼 */
    private String productId;

    /** 欲預留的商品數量 */
    private int quantity;
}
