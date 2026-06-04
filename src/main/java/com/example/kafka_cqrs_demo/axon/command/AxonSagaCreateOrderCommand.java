package com.example.kafka_cqrs_demo.axon.command;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * 建立訂單指令 (CreateOrderCommand)
 * * 指令 (Command) 代表業務的「意圖 (Intent)」。
 * 它是一個「不可變 (Immutable) 的數據載體」，用來表達「請系統執行某個動作」。
 * 當 Controller 發送此指令後，Axon 會根據 TargetAggregateIdentifier 找到對應的 Aggregate 進行處理。
 */
@Value // Lombok 的 @Value 會自動將所有欄位變為 private final，並產生 getter，保證資料不可變
public class AxonSagaCreateOrderCommand {

    /**
     * @TargetAggregateIdentifier 標記此欄位為「聚合根識別碼」。
     * * Axon 框架在收到此指令時，會自動讀取這個欄位的值，
     * 並用它來「鎖定」對應的 OrderAggregate 實體。
     * 如果這個訂單是新建的，Axon 就會創建一個新的 Aggregate 實例。
     */
    @TargetAggregateIdentifier
    private String orderId;

    /**
     * 商品 ID、數量與價格：這些是執行指令所需的業務參數。
     */
    private String productId;
    private int quantity;
    private long price;
}