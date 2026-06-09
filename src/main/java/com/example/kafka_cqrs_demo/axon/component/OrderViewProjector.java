package com.example.kafka_cqrs_demo.axon.component;

import com.example.kafka_cqrs_demo.axon.enums.OrderStatus;
import com.example.kafka_cqrs_demo.axon.event.OrderCancelledEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderPaidEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderStockReservedEvent;
import com.example.kafka_cqrs_demo.axon.repository.AxonOrderRepository;
import com.example.kafka_cqrs_demo.entity.AxonOrderEntity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.EventHandler;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * 訂單投影器 (Order View Projector)
 * <p>
 * 在 CQRS 架構中扮演查詢端 (Query Side) 的角色。
 * 本類別負責監聽 Axon 事件總線 (Event Bus) 中所發布的訂單領域事件，
 * 並將這些非結構化的事件流即時轉化、同步為適合快速查詢的讀取模型。
 * 同時寫入 Redis 快取層 (作高效能讀取) 以及 MySQL 資料庫的 t_axon_order 資料表 (作持久化備份)。
 * </p>
 */
@Slf4j
@Component
public class OrderViewProjector {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final AxonOrderRepository orderRepository;

    /**
     * 投影器的建構子。
     */
    public OrderViewProjector(StringRedisTemplate redisTemplate,
                              ObjectMapper objectMapper,
                              AxonOrderRepository orderRepository) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.orderRepository = orderRepository;
    }

    /**
     * 當訂單被建立時的事件處理器。
     * 同步初始化 MySQL 與 Redis 紀錄。
     *
     * @param event 訂單建立事件
     */
    @EventHandler
    @Transactional
    public void on(OrderCreatedEvent event) {
        log.info("[Projection] 正在同步訂單建立事件至 Redis & MySQL: {}", event.getOrderId());

        // 1. 同步至 MySQL 實體資料表
        try {
            AxonOrderEntity orderEntity = new AxonOrderEntity();
            orderEntity.setOrderId(event.getOrderId());
            orderEntity.setProductId(event.getProductId());
            orderEntity.setQuantity(event.getQuantity());
            orderEntity.setPrice(event.getPrice());
            orderEntity.setStatus(OrderStatus.CREATED.name());
            orderEntity.setCreateTime(LocalDateTime.now());
            orderEntity.setUserId(event.getUserId());
            orderRepository.save(orderEntity);
            log.info("[Projection] MySQL 訂單 {} 寫入成功", event.getOrderId());
        } catch (Exception e) {
            log.error("[Projection] 同步至 MySQL 失敗: {}", e.getMessage(), e);
        }

        // 2. 同步至 Redis 快取
        try {
            ObjectNode node = objectMapper.valueToTree(event);
            node.put("status", OrderStatus.CREATED.name());

            redisTemplate.opsForValue().set("order:" + event.getOrderId(), node.toString());
            redisTemplate.opsForSet().add("orders:all", event.getOrderId());

            log.info("[Projection] Redis 訂單 {} 寫入成功", event.getOrderId());
        } catch (Exception e) {
            log.error("[Projection] 同步至 Redis 失敗: {}", e.getMessage(), e);
        }
    }

    /**
     * 當訂單確認付款時的事件處理器。
     * 更新 MySQL 與 Redis 中的狀態。
     */
    @EventHandler
    @Transactional
    public void on(OrderPaidEvent event) {
        log.info("[Projection] 收到訂單付款成功事件，準備更新 Redis & MySQL: {}", event.getOrderId());
        updateMySQLStatus(event.getOrderId(), OrderStatus.PAID, null);
        updateOrderStatus(event.getOrderId(), OrderStatus.PAID, event.getReason());
    }

    /**
     * 當訂單被取消時的事件處理器。
     * 更新 MySQL 與 Redis 中的狀態。
     */
    @EventHandler
    @Transactional
    public void on(OrderCancelledEvent event) {
        log.info("[Projection] 收到訂單取消事件，準備更新 Redis & MySQL: {}", event.getOrderId());
        updateMySQLStatus(event.getOrderId(), OrderStatus.CANCELLED, event.getReason());
        updateOrderStatus(event.getOrderId(), OrderStatus.CANCELLED, event.getReason());
    }

    /**
     * 當訂單庫存成功扣減並確認預留時的事件處理器。
     * 更新 MySQL 與 Redis 中的狀態。
     */
    @EventHandler
    @Transactional
    public void on(OrderStockReservedEvent event) {
        log.info("[Projection] 庫存預留成功，更新 Redis & MySQL 狀態為 PENDING_PAYMENT: {}", event.getOrderId());
        updateMySQLStatus(event.getOrderId(), OrderStatus.PENDING_PAYMENT, null);
        updateOrderStatus(event.getOrderId(), OrderStatus.PENDING_PAYMENT, "付款中");
    }

    /**
     * 同步更新 MySQL 中的訂單狀態。
     */
    private void updateMySQLStatus(String orderId, OrderStatus status, String cancelReason) {
        try {
            Optional<AxonOrderEntity> orderOpt = orderRepository.findById(orderId);
            if (orderOpt.isPresent()) {
                AxonOrderEntity order = orderOpt.get();
                order.setStatus(status.name());
                if (cancelReason != null) {
                    order.setCancelReason(cancelReason);
                }
                orderRepository.save(order);
                log.info("[Projection] MySQL 訂單 {} 狀態成功更新為 {}", orderId, status);
            } else {
                log.warn("[Projection] MySQL 未找到訂單 {}，無法更新狀態", orderId);
            }
        } catch (Exception e) {
            log.error("[Projection] MySQL 訂單狀態更新失敗: ID {}, Error: {}", orderId, e.getMessage(), e);
        }
    }

    /**
     * 同步更新 Redis 中的訂單狀態。
     */
    private void updateOrderStatus(String orderId, OrderStatus status, String reason) {
        String key = "order:" + orderId;
        String json = redisTemplate.opsForValue().get(key);

        if (json != null) {
            try {
                JsonNode rootNode = objectMapper.readTree(json);
                ObjectNode objectNode = (ObjectNode) rootNode;

                objectNode.put("status", status.name());

                if (reason != null && !reason.isEmpty()) {
                    objectNode.put("cancelReason", reason);
                }

                redisTemplate.opsForValue().set(key, rootNode.toString());
                log.info("[Projection] Redis 訂單 {} 狀態成功更新為 {}", orderId, status);
            } catch (Exception e) {
                log.error("[Projection] Redis 訂單狀態更新失敗: ID {}, Error: {}", orderId, e.getMessage(), e);
            }
        } else {
            log.warn("[Projection] Redis 未找到訂單 {}，無法更新狀態", orderId);
        }
    }
}