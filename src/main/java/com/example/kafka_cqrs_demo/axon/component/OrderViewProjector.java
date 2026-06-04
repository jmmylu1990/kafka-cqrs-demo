package com.example.kafka_cqrs_demo.axon.component;

import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.EventHandler;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

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
     * @EventHandler 會自動註冊到 Axon 的事件總線上。
     * 每當 Aggregate 發布一個 OrderCreatedEvent，這個方法就會被自動呼叫。
     * * 這裡的邏輯是：將「事件流」轉化為「狀態視圖」。
     * 寫入邏輯 (Aggregate) 與 查詢邏輯 (Redis) 完全解耦。
     */
    @EventHandler
    public void on(OrderCreatedEvent event) {
        log.info("[Projection] 收到事件，正在同步至 Redis: {}", event.getOrderId());

        try {
            // 將事件物件轉為 JSON，以便前端能直接讀取或未來存入 Redis
            String jsonOrder = objectMapper.writeValueAsString(event);

            // 寫入 Redis：以 order:id 為 Key，將當前訂單快照儲存起來
            redisTemplate.opsForValue().set("order:" + event.getOrderId(), jsonOrder);

            log.info("Redis 同步成功！Key: order:{}", event.getOrderId());
        } catch (Exception e) {
            // 在這裡處理錯誤，若 Redis 掛了，日誌會幫你記錄，你可以之後再執行 Replay 事件來重補
            log.error("Redis 同步失敗: {}", e.getMessage(), e);
        }
    }
}