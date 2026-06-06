package com.example.kafka_cqrs_demo.axon.component;

import com.example.kafka_cqrs_demo.axon.enums.OrderStatus;
import com.example.kafka_cqrs_demo.axon.event.OrderCancelledEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderPaidEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.EventHandler;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 訂單投影器 (Order View Projector)
 * * [架構角色]：CQRS 架構中的「查詢端 (Query Side)」。
 * [職責]：
 * 1. 監聽 Axon 事件總線 (Event Bus) 中的領域事件。
 * 2. 將非結構化的「事件流」轉化為結構化的「讀取模型 (Read Model)」。
 * 3. 確保 Redis 中的查詢數據與 Event Store 中的真實狀態保持一致性。
 */
@Slf4j
@Component
public class OrderViewProjector {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public OrderViewProjector(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 當訂單被建立時，初始化 Redis 紀錄並建立索引。
     * * @param event 訂單建立事件
     */
    @EventHandler
    public void on(OrderCreatedEvent event) {
        log.info("[Projection] 正在同步訂單建立事件至 Redis: {}", event.getOrderId());

        try {
            // 1. 將事件物件轉為 JSON，並強制寫入初始狀態 CREATED
            ObjectNode node = objectMapper.valueToTree(event);
            node.put("status", OrderStatus.CREATED.name());

            redisTemplate.opsForValue().set("order:" + event.getOrderId(), node.toString());

            // 2. 維護全局訂單清單索引 (Set)
            // 讓後續的 "Get All Orders" 查詢能以 O(1) 的複雜度取得所有訂單 ID
            redisTemplate.opsForSet().add("orders:all", event.getOrderId());

            log.info("Redis 同步完成，訂單 {} 已初始化並加入索引", event.getOrderId());
        } catch (Exception e) {
            log.error("Redis 初始化同步失敗: {}", e.getMessage(), e);
        }
    }

    /**
     * 當訂單付款事件發生時，更新 Redis 中的狀態。
     */
    @EventHandler
    public void on(OrderPaidEvent event) {
        updateOrderStatus(event.getOrderId(), OrderStatus.PAID);
    }

    /**
     * 當訂單取消事件發生時，更新 Redis 中的狀態。
     */
    @EventHandler
    public void on(OrderCancelledEvent event) {
        updateOrderStatus(event.getOrderId(), OrderStatus.CANCELLED);
    }

    /**
     * 通用的狀態更新邏輯
     * 封裝了對 Redis 讀取、JSON 解析、節點修改與重新寫入的處理流程。
     * * @param orderId 訂單識別碼
     * @param status 要更新的目標狀態 (OrderStatus 枚舉)
     */
    private void updateOrderStatus(String orderId, OrderStatus status) {
        String key = "order:" + orderId;
        String json = redisTemplate.opsForValue().get(key);

        if (json != null) {
            try {
                // 1. 解析 JSON 字串為 JsonNode，便於操作屬性
                JsonNode rootNode = objectMapper.readTree(json);

                // 2. 更新 status 欄位 (透過 ObjectNode 進行操作)
                ((ObjectNode) rootNode).put("status", status.name());

                // 3. 將修改後的物件寫回 Redis，完成資料同步
                redisTemplate.opsForValue().set(key, rootNode.toString());
                log.info("Redis 更新成功: 訂單 {} 狀態變更為 {}", orderId, status);
            } catch (Exception e) {
                log.error("更新 Redis 狀態失敗: ID {}, Error: {}", orderId, e.getMessage());
            }
        } else {
            // 若 Redis 中找不到該 Key，代表投影層與事件流可能出現了非預期的資料不一致
            log.warn("⚠嘗試更新不存在的訂單狀態: {}", orderId);
        }
    }
}