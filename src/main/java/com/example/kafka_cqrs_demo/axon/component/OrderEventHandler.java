package com.example.kafka_cqrs_demo.axon.component;

import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.EventHandler;
import org.springframework.stereotype.Component;

/**
 * 訂單領域事件處理器 (Order Event Handler)
 * <p>
 * 本類別用於處理與訂單相關的非同步/同步領域事件。
 * 相比於用於維護查詢模型 (Read Model) 的投影器 (Projector)，此 EventHandler 主要用於執行一般性業務輔助操作，
 * 例如向使用者發送通知、觸發外部第三方 API 呼叫，或是記錄安全審計日誌。
 * </p>
 */
@Slf4j
@Component
public class OrderEventHandler {

    /**
     * 當訂單被成功建立時觸發的事件監聽方法。
     *
     * @param event 訂單建立事件
     */
    @EventHandler
    public void on(OrderCreatedEvent event) {
        log.info("[EventHandler] 成功監聽事件，訂單ID: " + event.getOrderId());
    }
}
