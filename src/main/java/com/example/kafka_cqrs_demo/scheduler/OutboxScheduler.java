package com.example.kafka_cqrs_demo.scheduler;

import com.example.kafka_cqrs_demo.entity.OutboxEntity;
import com.example.kafka_cqrs_demo.repository.OutboxRepository;
import com.example.kafka_cqrs_demo.service.OutboxService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 發件箱背景補償排程器 (Outbox Scheduler)
 * <p>
 * 定期輪詢 t_outbox 中為 PENDING 狀態的訊息進行重試發送，
 * 並透過 Redisson 分散式鎖確保在多服務部署時，同一時間只有一個實例在執行發送。
 * </p>
 */
@Component
@Slf4j
public class OutboxScheduler {

    private final OutboxRepository outboxRepository;
    private final OutboxService outboxService;
    private final RedissonClient redissonClient;

    public OutboxScheduler(OutboxRepository outboxRepository,
                           OutboxService outboxService,
                           RedissonClient redissonClient) {
        this.outboxRepository = outboxRepository;
        this.outboxService = outboxService;
        this.redissonClient = redissonClient;
    }

    /**
     * 每 5 秒執行一次發件箱輪詢補償。
     */
    @Scheduled(fixedDelay = 5000)
    public void processPendingOutboxEvents() {
        String lockKey = "lock:outbox:scheduler";
        RLock lock = redissonClient.getLock(lockKey);

        boolean locked = false;
        try {
            // 嘗試獲取鎖，若獲取失敗直接退出 (代表其他節點已在處理中)
            locked = lock.tryLock(0, TimeUnit.SECONDS);
            if (locked) {
                // 1. 獲取前 50 筆 PENDING 訊息，按時間排序
                List<OutboxEntity> pendingEvents = outboxRepository.findTop50ByStatusOrderByCreateTimeAsc("PENDING");

                if (!pendingEvents.isEmpty()) {
                    log.info("[OutboxScheduler] 發現 {} 筆待處理的 Outbox 訊息，開始進行發送...", pendingEvents.size());
                    for (OutboxEntity event : pendingEvents) {
                        try {
                            outboxService.publishEvent(event);
                        } catch (Exception e) {
                            log.error("[OutboxScheduler] 處理 Outbox 任務失敗。ID: {}, 原因: {}", event.getId(), e.getMessage());
                        }
                    }
                    log.info("[OutboxScheduler] 待處理訊息處理階段結束。");
                }
            }
        } catch (InterruptedException e) {
            log.warn("[OutboxScheduler] 獲取排程鎖執行緒被中斷", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("[OutboxScheduler] 背景發送排程發生異常: {}", e.getMessage(), e);
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
