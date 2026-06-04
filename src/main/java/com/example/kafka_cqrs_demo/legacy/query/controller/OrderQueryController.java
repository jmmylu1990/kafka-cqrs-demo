package com.example.kafka_cqrs_demo.legacy.query.controller;

import com.example.kafka_cqrs_demo.legacy.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.legacy.query.service.OrderQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/view/orders")
public class OrderQueryController {
    private final OrderQueryService orderQueryService;

    @Autowired
    public OrderQueryController(OrderQueryService orderQueryService) {
        this.orderQueryService = orderQueryService;
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderCreatedEvent> getOrder(@PathVariable String orderId) {
        OrderCreatedEvent order = orderQueryService.getOrderById(orderId);

        // 如果還沒同步完成或查無此訂單，回傳 404
        if (order == null) {
            return ResponseEntity.notFound().build();
        }

        // 查到資料回傳 200 OK 與 JSON 內容
        return ResponseEntity.ok(order);
    }
}
