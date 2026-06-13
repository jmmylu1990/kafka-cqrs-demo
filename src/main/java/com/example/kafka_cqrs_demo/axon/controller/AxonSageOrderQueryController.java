package com.example.kafka_cqrs_demo.axon.controller;

import com.example.kafka_cqrs_demo.axon.service.OrderQueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

/**
 * 訂單查詢端 API 控制器 (Order Query Controller)
 * <p>
 * 作為 CQRS (Command Query Responsibility Segregation) 架構下的讀取端 (Query Side) 入口。
 * 本控制器重構後僅注入 {@link OrderQueryService}，不再直接依賴 Redis、Redisson、Jackson、Metrics 等低層技術，
 * 遵循單一職責原則 (SRP) 與依賴反轉原則 (DIP)。
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("/axonsaga/api/orders")
public class AxonSageOrderQueryController {

    private final OrderQueryService orderQueryService;

    /**
     * 查詢控制器的建構子。
     */
    public AxonSageOrderQueryController(OrderQueryService orderQueryService) {
        this.orderQueryService = orderQueryService;
    }

    /**
     * 根據訂單唯一識別碼查詢訂單詳情。
     *
     * @param orderId 訂單唯一識別碼 (UUID)
     * @return 訂單的 JSON 詳細資料，若找不到則返回「訂單不存在」
     */
    @GetMapping("/{orderId}")
    public String getOrder(@PathVariable String orderId) {
        log.info("接收到查詢訂單請求，ID: {}", orderId);
        return orderQueryService.getOrder(orderId);
    }

    /**
     * 查詢所有已建立的訂單 ID 列表。
     *
     * @return 包含所有訂單 ID 的 Set 集合
     */
    @GetMapping("/all")
    public Set<String> getAllOrderIds() {
        log.info("接收到查詢所有訂單 ID 列表請求");
        return orderQueryService.getAllOrderIds();
    }
}
