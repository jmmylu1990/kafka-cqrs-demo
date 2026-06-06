package com.example.kafka_cqrs_demo.axon.dto;

import lombok.Data;

/**
 * 取消訂單請求資料傳輸物件 (Cancel Order Request DTO)
 * <p>
 * 用於接收前端傳入的取消訂單請求參數與取消原因。
 * </p>
 */
@Data
public class CancelOrderRequest {

    /** 欲取消的訂單唯一識別碼 */
    private String orderId;

    /** 取消訂單的理由說明，例如「顧客後悔」、「規格選錯」等 */
    private String reason;
}