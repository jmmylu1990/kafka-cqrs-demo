package com.example.kafka_cqrs_demo.legacy.command.controller;

import com.example.kafka_cqrs_demo.legacy.command.dto.CreateOrderRequest;
import com.example.kafka_cqrs_demo.legacy.command.dto.OrderCommandResponse;
import com.example.kafka_cqrs_demo.legacy.command.service.OrderCommandService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 傳統模式訂單寫入端 API 控制器 (Legacy Order Command Controller)
 * <p>
 * 用於處理傳統手動 Kafka CQRS 模式下的訂單寫入 HTTP 請求。
 * 接收使用者建立訂單的請求後，呼叫服務層發送事件至 Kafka，並立即返回 HTTP 202 Accepted 狀態，
 * 告知呼叫端系統已接受訂單並在背景非同步處理中。
 * </p>
 */
@RestController
@RequestMapping("/api/orders")
public class OrderCommandController {

    private final OrderCommandService orderCommandService;

    /**
     * 建構子。
     *
     * @param orderCommandService 傳統模式訂單寫入服務
     */
    @Autowired
    public OrderCommandController(OrderCommandService orderCommandService) {
        this.orderCommandService = orderCommandService;
    }

    /**
     * 處理建立訂單的 POST 請求。
     *
     * @param request 驗證通過的訂單建立資料請求
     * @return 結構化 JSON 回應 ResponseEntity
     */
    @PostMapping
    public ResponseEntity<OrderCommandResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        String productId = request.getProductId();
        int quantity = request.getQuantity();
        long price = request.getPrice();

        // 呼叫 Service 建立訂單，此時內部會發送事件到 Kafka 主題，並回傳產生的訂單 ID
        String orderId = orderCommandService.createOrder(productId, quantity, price);

        // 回傳結構化的 JSON，明確告知前端目前狀態是 PENDING 待處理
        OrderCommandResponse responseBody = new OrderCommandResponse(
            orderId,
            "PENDING",
            "訂單已接收，系統正非同步處理中，請拿此 orderId 進行輪詢。"
        );

        return ResponseEntity.accepted().body(responseBody);
    }
}