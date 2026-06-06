package com.example.kafka_cqrs_demo.axon.controller;

import com.example.kafka_cqrs_demo.axon.command.AxonSagaCreateOrderCommand;
import com.example.kafka_cqrs_demo.axon.command.CancelOrderCommand;
import com.example.kafka_cqrs_demo.axon.command.ConfirmPaymentCommand;
import com.example.kafka_cqrs_demo.axon.dto.CancelOrderRequest;
import com.example.kafka_cqrs_demo.axon.dto.PayOrderRequest;
import com.example.kafka_cqrs_demo.legacy.command.dto.CreateOrderRequest;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
     *
     * @param commandGateway 由 Spring Boot 自動注入的 Axon CommandGateway 實例
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

        // 將 API 請求包裝為領域指令
        AxonSagaCreateOrderCommand command = new AxonSagaCreateOrderCommand(
            orderId,
            request.getProductId(),
            request.getQuantity(),
            request.getPrice()
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
     * @param request 包含訂單 ID 的付款確認請求 DTO
     * @return 包含付款處理結果的 CompletableFuture 物件
     */
    @PostMapping("/pay")
    public CompletableFuture<String> payOrder(@RequestBody PayOrderRequest request) {
        log.info("接收到訂單付款請求，訂單 ID: {}", request.getOrderId());
        return commandGateway.send(new ConfirmPaymentCommand(request.getOrderId()));
    }

    /**
     * 處理取消訂單的 REST API 請求。
     *
     * @param request 包含訂單 ID 與取消原因的請求 DTO
     * @return 包含取消處理結果的 CompletableFuture 物件
     */
    @PostMapping("/cancel")
    public CompletableFuture<String> cancelOrder(@RequestBody CancelOrderRequest request) {
        log.info("接收到訂單取消請求，訂單 ID: {}, 原因: {}", request.getOrderId(), request.getReason());
        return commandGateway.send(new CancelOrderCommand(request.getOrderId(), request.getReason()));
    }
}