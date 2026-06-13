package com.example.kafka_cqrs_demo.axon.controller;

import com.example.kafka_cqrs_demo.axon.command.AxonSagaCreateOrderCommand;
import com.example.kafka_cqrs_demo.axon.command.CancelOrderCommand;
import com.example.kafka_cqrs_demo.axon.command.ConfirmPaymentCommand;
import com.example.kafka_cqrs_demo.axon.command.ProcessPaymentCommand;
import com.example.kafka_cqrs_demo.legacy.command.dto.CreateOrderRequest;
import com.example.kafka_cqrs_demo.axon.dto.CancelOrderRequest;
import com.example.kafka_cqrs_demo.axon.dto.PayOrderRequest;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * 訂單寫入端 API 控制器 (Order Command Controller)
 * <p>
 * 作為 CQRS (Command Query Responsibility Segregation) 架構下的寫入端入口。
 * 本控制器完全不進行任何資料庫的直接操作，其唯一職責是接收 HTTP 請求，
 * 將其封裝轉換為具備業務意圖的「指令 (Command)」，並透過 Axon 的 {@link CommandGateway} 發布至系統中。
 * 實際的商業邏輯驗證與持久化將由對應的聚合根 (Aggregate Root) 異步處理。
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("/axonsaga/api/orders")
public class AxonSageOrderCommandController {

    /**
     * Axon 框架提供的指令閘道器。
     * 負責將指令路由到正確的領域聚合根 (Aggregate Root)。
     */
    private final CommandGateway commandGateway;

    /**
     * 控制器的建構子。
     */
    @Autowired
    public AxonSageOrderCommandController(CommandGateway commandGateway) {
        this.commandGateway = commandGateway;
    }

    /**
     * 處理建立新訂單的 REST API 請求。
     * <p>
     * 本方法在接收到請求後，會產生一個全新的隨機 UUID 作為訂單 ID，
     * 並建立一個 {@link AxonSagaCreateOrderCommand} 送入系統。
     * 使用 {@link CompletableFuture} 實現非同步等待，當 Axon 後台處理完畢後，返回結果。
     * </p>
     *
     * @param request 包含購買產品與價錢等資訊的請求資料傳輸物件 (DTO)
     * @return 包含新建訂單 ID（若成功）或錯誤說明的 CompletableFuture 物件
     */
    @PostMapping
    public CompletableFuture<String> createOrder(@RequestBody CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        String userId = request.getUserId() != null && !request.getUserId().isBlank()
            ? request.getUserId()
            : "USER-001";

        // 將 API 請求包裝為領域指令
        AxonSagaCreateOrderCommand command = new AxonSagaCreateOrderCommand(
            orderId,
            request.getProductId(),
            request.getQuantity(),
            request.getPrice(),
            userId
        );

        log.info("DEBUG - 指令類別路徑: {}" , command.getClass().getName());

        // 發送指令並非同步處理回覆
        return commandGateway.send(command)
            .handle((result, exception) -> {
                if (exception != null) {
                    // 獲取包裝異常中的真實根源例外
                    Throwable cause = exception.getCause() != null ? exception.getCause() : exception;
                    log.error("Command 發送失敗, ID: {}, 錯誤: {}", orderId, cause.getMessage(), cause);
                    return "Error: " + cause.getMessage();
                }

                log.info("Command 發送成功，返回: " + result);
                return result.toString();
            });
    }

    /**
     * 處理付款確認的 REST API 請求。
     *
     * @param request 包含付款確認資訊的請求 DTO
     * @return 包含付款處理結果的 ResponseEntity
     */
    @PostMapping("/pay")
    public ResponseEntity<Map<String, Object>> payOrder(@RequestBody PayOrderRequest request) {
        String orderId = request.getOrderId();
        String userId = request.getUserId() != null && !request.getUserId().isBlank()
                ? request.getUserId()
                : "USER-001";
        log.info("接收到訂單付款請求，訂單 ID: {}, 使用者 ID: {}, 付款方式: {}", orderId, userId, request.getPaymentMethod());
        
        // 1. 異步發送 Command，不阻塞等待背景 Saga/金流 API 處理
        commandGateway.send(new ProcessPaymentCommand(orderId, userId));
        
        // 2. 建立查詢狀態的 Location URL
        String queryUrl = "http://localhost:8081/axonsaga/api/orders/" + orderId;
        
        // 3. 封裝 REST Response Body
        Map<String, Object> responseBody = new HashMap<>();
        responseBody.put("status", "PROCESSING");
        responseBody.put("message", "付款扣款流程已啟動，請至 Location 標頭或 queryUrl 中的網址查詢最終結果");
        responseBody.put("orderId", orderId);
        responseBody.put("queryUrl", queryUrl);
        
        // 4. 回傳 HTTP 202 Accepted 與 Location 標頭
        return ResponseEntity.accepted()
                .header(HttpHeaders.LOCATION, queryUrl)
                .body(responseBody);
    }

    /**
     * 處理取消訂單的 REST API 請求。
     *
     * @param request 包含取消訂單識別碼及原因的請求 DTO
     * @return 包含取消處理結果的 CompletableFuture 物件
     */
    @PostMapping("/cancel")
    public CompletableFuture<String> cancelOrder(@RequestBody CancelOrderRequest request) {
        String orderId = request.getOrderId();
        String reason = request.getReason() != null && !request.getReason().isBlank()
                ? request.getReason()
                : "用戶取消";
        log.info("接收到訂單取消請求，訂單 ID: {}, 原因: {}", orderId, reason);
        return commandGateway.send(new CancelOrderCommand(orderId, reason));
    }
}