package com.example.kafka_cqrs_demo.legacy.command.controller;

import com.example.kafka_cqrs_demo.legacy.command.dto.CreateOrderRequest;
import com.example.kafka_cqrs_demo.legacy.command.dto.OrderCommandResponse; // 引入剛建立的 DTO
import com.example.kafka_cqrs_demo.legacy.command.service.OrderCommandService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
public class OrderCommandController {

    private final OrderCommandService orderCommandService;

    @Autowired
    public OrderCommandController(OrderCommandService orderCommandService) {
        this.orderCommandService = orderCommandService;
    }

    @PostMapping
    public ResponseEntity<OrderCommandResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        String productId = request.getProductId();
        int quantity = request.getQuantity();
        long price = request.getPrice();

        // 呼叫 Service 建立訂單，此時內部會發送 Kafka 並塞入 Redis cache
        String orderId = orderCommandService.createOrder(productId, quantity, price);

        // 🔥 業界標準：回傳結構化的 JSON，明確告知前端目前狀態是 PENDING
        OrderCommandResponse responseBody = new OrderCommandResponse(
            orderId,
            "PENDING",
            "訂單已接收，系統正非同步處理中，請拿此 orderId 進行輪詢。"
        );

        return ResponseEntity.accepted().body(responseBody);
    }
}