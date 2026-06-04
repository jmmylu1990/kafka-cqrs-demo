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
 * CQRS入口。
 * 不直接操作資料庫，而是將請求轉譯為「意圖 (Command)」發送給 Axon。
 */
@Slf4j
@RestController
@RequestMapping("/axonsaga/api/orders")
public class AxonSageOrderCommandController {

    /**
     * CommandGateway 是 Axon 的核心樞紐。
     * 它就像是「訊息調度員」，負責將 Command 路由到正確的 Aggregate 實體。
     */
    private final CommandGateway commandGateway;

    @Autowired
    public AxonSageOrderCommandController(CommandGateway commandGateway) {
        this.commandGateway = commandGateway;
    }

    /**
     * 建立訂單的 API
     * @param request 前端傳入的訂單原始參數
     * @return CompletableFuture 非同步等待 Axon 處理完後回傳結果
     */
    @PostMapping
    public CompletableFuture<String> createOrder(@RequestBody CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        // 建立指令
        AxonSagaCreateOrderCommand command = new AxonSagaCreateOrderCommand(
            orderId,
            request.getProductId(),
            request.getQuantity(),
            request.getPrice()
        );

        log.info("DEBUG - 指令類別路徑: {}" , command.getClass().getName());

        // 發送指令並處理結果
        return commandGateway.send(command)
            .handle((result, exception) -> {
                if (exception != null) {
                    // 這裡的 exception.getCause() 通常包含真正的錯誤訊息
                    Throwable cause = exception.getCause() != null ? exception.getCause() : exception;
                    // 使用 log.error，並直接傳入 throwable 物件
                    // SLF4J 會自動處理堆疊追蹤，且它會正確進入 Log 檔案或日誌伺服器
                    log.error("Command 發送失敗, ID: {}, 錯誤: {}", orderId, cause.getMessage(), cause);

                    return "Error: " + cause.getMessage();
                }

                log.info(" Command 發送成功，返回: " + result);
                return result.toString();
            });
    }

    @PostMapping("/pay")
    public CompletableFuture<String> payOrder(@RequestBody PayOrderRequest request) {

        return commandGateway.send(new ConfirmPaymentCommand(request.getOrderId()));
    }

    @PostMapping("/cancel")
    public CompletableFuture<String> cancelOrder(@RequestBody CancelOrderRequest request) {
        return commandGateway.send(new CancelOrderCommand(request.getOrderId(), request.getReason()));
    }
}