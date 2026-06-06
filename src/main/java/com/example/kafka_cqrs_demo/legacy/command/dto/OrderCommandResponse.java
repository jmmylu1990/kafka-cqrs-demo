package com.example.kafka_cqrs_demo.legacy.command.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 傳統模式訂單指令回應 DTO (Legacy Order Command Response)
 * <p>
 * 用於封裝傳統模式下 API 接收到建立訂單請求後的即時非同步處理響應資訊。
 * </p>
 */
@Data
@AllArgsConstructor
public class OrderCommandResponse {

    /** 系統產生的訂單唯一識別碼 (UUID) */
    private String orderId;

    /** 訂單的當前同步狀態（例如 "PENDING"） */
    private String status;

    /** 系統回傳的指示說明文字 */
    private String message;
}
