package com.example.kafka_cqrs_demo.axon.component;

import com.example.kafka_cqrs_demo.axon.enums.OrderStatus;
import com.example.kafka_cqrs_demo.axon.event.OrderCancelledEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderPaidEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderStockReservedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.EventHandler;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 訂單投影器 (Order View Projector)
 * <p>
 * 在 CQRS (Command Query Responsibility Segregation) 架構中扮演查詢端 (Query Side) 的角色。
 * 本類別負責監聽 Axon 事件總線 (Event Bus) 中所發布的訂單領域事件，
 * 並將這些非結構化的事件流 (Event Stream) 即時轉化、同步為適合快速查詢的結構化讀取模型 (Read Model)，
 * 此處以 Redis 作為高性能的讀取快取資料庫。
 * </p>
 */
@Slf4j
@Component
public class OrderViewProjector {

    /** 用於操作 Redis 字串資料的 Template 實例 */
    private final StringRedisTemplate redisTemplate;

    /** Jackson 的 ObjectMapper，用於 JSON 資料與 Java 物件的互相轉換 */
    private final ObjectMapper objectMapper;

    /**
     * 投影器的建構子。
     *
     * @param redisTemplate Spring Boot 自動配置的 StringRedisTemplate
     * @param objectMapper 全域統一設定的 ObjectMapper
     */
    public OrderViewProjector(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 當訂單被建立時的事件處理器。
     * 監聽到 OrderCreatedEvent 後，初始化 Redis 紀錄並維護全局訂單清單索引（Set 結構）。
     *
     * @param event 訂單建立事件
     */
    @EventHandler
    public void on(OrderCreatedEvent event) {
        log.info("[Projection] 正在同步訂單建立事件至 Redis: {}", event.getOrderId());

        try {
            // 將事件物件轉成 Jackson ObjectNode 節點，並手動塞入初始狀態 CREATED
            ObjectNode node = objectMapper.valueToTree(event);
            node.put("status", OrderStatus.CREATED.name());

            // 寫入 Redis，鍵名格式為 order:{orderId}
            redisTemplate.opsForValue().set("order:" + event.getOrderId(), node.toString());

            // 將此訂單 ID 新增到 orders:all 集合中，供分頁或列出所有訂單 ID 使用
            redisTemplate.opsForSet().add("orders:all", event.getOrderId());

            log.info("Redis 同步完成，訂單 {} 已初始化並加入索引", event.getOrderId());
        } catch (Exception e) {
            log.error("Redis 初始化同步失敗: {}", e.getMessage(), e);
        }
    }

    /**
     * 當訂單確認付款時的事件處理器。
     * 監聽到 OrderPaidEvent 後，將 Redis 中的訂單狀態更新為 PAID。
     *
     * @param event 訂單付款成功事件
     */
    @EventHandler
    public void on(OrderPaidEvent event) {
        log.info("[Projection] 收到訂單付款成功事件，準備更新 Redis: {}", event.getOrderId());
        updateOrderStatus(event.getOrderId(), OrderStatus.PAID, event.getReason());
    }

    /**
     * 當訂單被取消時的事件處理器。
     * 監聽到 OrderCancelledEvent 後，將 Redis 中的訂單狀態更新為 CANCELLED，並記錄取消原因。
     *
     * @param event 訂單取消事件
     */
    @EventHandler
    public void on(OrderCancelledEvent event) {
        log.info("[Projection] 收到訂單取消事件，準備更新 Redis: {}", event.getOrderId());
        updateOrderStatus(event.getOrderId(), OrderStatus.CANCELLED, event.getReason());
    }

    /**
     * 當訂單庫存成功扣減並確認預留時的事件處理器。
     * 監聽到 OrderStockReservedEvent 後，將 Redis 中的訂單狀態更新為 PENDING_PAYMENT。
     *
     * @param event 訂單庫存預留成功事件
     */
    @EventHandler
    public void on(OrderStockReservedEvent event) {
        log.info("[Projection] 庫存預留成功，更新 Redis 狀態為 PENDING_PAYMENT: {}", event.getOrderId());
        updateOrderStatus(event.getOrderId(), OrderStatus.PENDING_PAYMENT, "付款中");
    }

    /**
     * 通用的 Redis 訂單狀態更新業務邏輯。
     * 負責讀取 Redis 舊有 JSON 資料、反序列化為 ObjectNode、修改狀態欄位及寫入回 Redis。
     *
     * @param orderId 訂單唯一識別碼
     * @param status 欲更新之目標狀態
     * @param reason 更新狀態之理由（選填，例如取消原因或付款狀態備註）
     */
    private void updateOrderStatus(String orderId, OrderStatus status, String reason) {
        String key = "order:" + orderId;
        String json = redisTemplate.opsForValue().get(key);

        if (json != null) {
            try {
                JsonNode rootNode = objectMapper.readTree(json);
                ObjectNode objectNode = (ObjectNode) rootNode;

                // 更新 JSON 物件的狀態屬性
                objectNode.put("status", status.name());

                // 若傳入的理由描述不為空，則更新對應欄位
                if (reason != null && !reason.isEmpty()) {
                    objectNode.put("cancelReason", reason);
                }

                // 重新序列化寫入 Redis
                redisTemplate.opsForValue().set(key, rootNode.toString());
                log.info("Redis 更新成功: 訂單 {} 狀態變更為 {}", orderId, status);
            } catch (Exception e) {
                log.error("更新 Redis 狀態失敗: ID {}, Error: {}", orderId, e.getMessage());
            }
        } else {
            log.warn("嘗試更新不存在的訂單狀態: {}", orderId);
        }
    }
}