package com.example.kafka_cqrs_demo.axon.command;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * 建立訂單指令 (Create Order Command)
 * <p>
 * 指令 (Command) 代表業務的「意圖 (Intent)」。
 * 本類別是一個不可變的資料載體，用來表達「在系統中建立一筆新訂單」的請求。
 * 當 API 控制器發送此指令後，Axon 會根據 TargetAggregateIdentifier 將其路由，
 * 並在此訂單 ID 的對應聚合根實例上執行建立處理。
 * </p>
 */
@Value
public class AxonSagaCreateOrderCommand {

    /**
     * 目標聚合識別碼
     * Axon 框架在收到此指令時，會自動讀取此欄位的值，
     * 並用來鎖定或建立對應的訂單聚合根實例。
     */
    @TargetAggregateIdentifier
    private String orderId;

    /** 訂單所購買的產品唯一識別碼 */
    private String productId;

    /** 訂單購買的產品數量 */
    private int quantity;

    /** 訂單的商品單價，單位為最小貨幣單位 */
    private long price;
}