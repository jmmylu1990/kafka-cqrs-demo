package com.example.kafka_cqrs_demo.legacy.query.controller;

import com.example.kafka_cqrs_demo.legacy.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.legacy.query.service.OrderQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 傳統模式讀取端 API 控制器 (Legacy Order Query Controller)
 * <p>
 * 提供傳統 CQRS 模式下的讀取 API 接口。前端或輪詢端可以呼叫此接口以查詢最新狀態。
 * 其只從讀取端的 Service 抓取資料，與寫入端的資料庫及寫入服務無直接依賴。
 * </p>
 */
@RestController
@RequestMapping("/api/view/orders")
public class OrderQueryController {

    private final OrderQueryService orderQueryService;

    /**
     * 建構子。
     *
     * @param orderQueryService 傳統模式讀取端服務
     */
    @Autowired
    public OrderQueryController(OrderQueryService orderQueryService) {
        this.orderQueryService = orderQueryService;
    }

    /**
     * 根據訂單唯一識別碼查詢訂單資訊。
     *
     * @param orderId 訂單唯一識別碼
     * @return 包含訂單事件資料的 ResponseEntity，查無此資料時回傳 404
     */
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
