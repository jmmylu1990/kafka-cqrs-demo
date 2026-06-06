package com.example.kafka_cqrs_demo.axon.dto;

import lombok.Data;

/**
 * 訂單付款請求資料傳輸物件 (Pay Order Request DTO)
 * <p>
 * 用於接收前端傳入的付款確認請求參數。
 * </p>
 */
@Data
public class PayOrderRequest {

    /** 欲付款的訂單唯一識別碼 */
    private String orderId;

    /** 付款管道/方法，例如 CREDIT_CARD、LINE_PAY 等 */
    private String paymentMethod;
}
