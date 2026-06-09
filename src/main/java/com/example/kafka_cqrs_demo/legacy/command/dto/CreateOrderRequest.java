package com.example.kafka_cqrs_demo.legacy.command.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * 訂單建立請求資料傳輸物件 (Create Order Request DTO)
 * <p>
 * 接收使用者從前端或 REST API 傳入的原始建立訂單資訊，並使用 Jakarta Bean Validation 進行基礎欄位格式校驗。
 * </p>
 */
@Data
public class CreateOrderRequest {

    /** 欲購買商品的產品唯一識別碼，不可為空 */
    @NotBlank(message = "商品ID不能為空")
    private String productId;

    /** 購買數量，限制最小為 1 */
    @Min(value = 1, message = "數量至少為 1")
    private int quantity;

    /** 商品單價，限制不可為負數 */
    @Min(value = 0, message = "價格不能為負數")
    private long price;

    /** 使用者唯一識別碼 */
    private String userId;
}
