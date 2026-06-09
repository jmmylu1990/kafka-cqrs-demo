package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.command.ReserveStockCommand;
import com.example.kafka_cqrs_demo.axon.config.InventoryKafkaConfig;
import com.example.kafka_cqrs_demo.axon.dto.InventorySyncEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderCancelledEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderPaidEvent;
import com.example.kafka_cqrs_demo.axon.event.StockFailedEvent;
import com.example.kafka_cqrs_demo.axon.event.StockReservedEvent;
import com.example.kafka_cqrs_demo.axon.repository.AxonInventoryRepository;
import com.example.kafka_cqrs_demo.axon.repository.AxonStockReservationRepository;
import com.example.kafka_cqrs_demo.entity.AxonStockReservationEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 庫存服務 (Inventory Service) - Redisson 分散式鎖版
 * <p>
 * 負責處理來自 Saga 流程的庫存扣減/預留請求。
 * 本服務採用「Redisson 分散式鎖 + Kafka 異步同步寫回 MySQL」的模式：
 * 1. 收到 {@link ReserveStockCommand} 指令後，使用 Redisson 的 RLock 鎖定商品 ID，確保高併發下庫存檢查與扣減的執行緒安全。
 * 2. 扣減成功後，發布 StockReservedEvent 事件驅動 Saga，同時發送庫存同步訊息至 Kafka。
 * 3. 監聽訂單付款成功與取消事件，並同樣透過商品分散式鎖安全地扣減或釋放預留庫存，隨後發送同步訊息至 Kafka。
 * </p>
 */
@Slf4j
@Service
public class InventoryService {

    private final EventGateway eventGateway;
    private final AxonInventoryRepository inventoryRepository;
    private final AxonStockReservationRepository reservationRepository;
    private final RedissonClient redissonClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 庫存服務建構子。
     */
    public InventoryService(EventGateway eventGateway,
                            AxonInventoryRepository inventoryRepository,
                            AxonStockReservationRepository reservationRepository,
                            RedissonClient redissonClient,
                            KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper) {
        this.eventGateway = eventGateway;
        this.inventoryRepository = inventoryRepository;
        this.reservationRepository = reservationRepository;
        this.redissonClient = redissonClient;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 處理預留庫存指令的 CommandHandler。
     * <p>
     * 透過 Redisson 分散式鎖鎖定商品 ID，並在臨界區內執行 Redis 庫存判斷與扣減。
     * </p>
     */
    @CommandHandler
    public void handle(ReserveStockCommand command) {
        log.info("[InventoryService] 收到庫存預留請求 (Redisson): orderId={}, productId={}, quantity={}",
                command.getOrderId(), command.getProductId(), command.getQuantity());

        String productId = command.getProductId();
        String orderId = command.getOrderId();
        int qty = command.getQuantity();

        // 鎖定商品 ID 以保護庫存更新
        String lockKey = "lock:product:" + productId;
        RLock lock = redissonClient.getLock(lockKey);

        boolean locked = false;
        try {
            // 嘗試獲取分散式鎖，最多等待 5 秒，使用 Watchdog 自動續期
            locked = lock.tryLock(5, TimeUnit.SECONDS);
            if (locked) {
                String stockKey = "product:" + productId + ":stock";
                String reservedKey = "product:" + productId + ":reserved";
                String orderResKey = "order:" + orderId + ":reservation";

                // 使用 StringCodec 確保與原始 StringRedisTemplate/Lettuce 的字串讀寫格式相容
                RBucket<String> stockBucket = redissonClient.getBucket(stockKey, StringCodec.INSTANCE);
                RBucket<String> reservedBucket = redissonClient.getBucket(reservedKey, StringCodec.INSTANCE);
                RMap<String, String> orderResMap = redissonClient.getMap(orderResKey, StringCodec.INSTANCE);

                // 1. 檢查商品是否存在
                if (!stockBucket.isExists()) {
                    log.warn("[InventoryService] 預留失敗：商品 {} 在 Redis 中不存在", productId);
                    eventGateway.publish(new StockFailedEvent(orderId, "商品不存在"));
                    return;
                }

                // 2. 檢查可用庫存是否充足
                int currentStock = Integer.parseInt(stockBucket.get());
                if (currentStock < qty) {
                    log.warn("[InventoryService] 預留失敗：商品 {} 庫存不足。剩餘庫存: {}, 請求數量: {}",
                            productId, currentStock, qty);
                    eventGateway.publish(new StockFailedEvent(orderId, "庫存不足"));
                    return;
                }

                // 3. 扣減可用庫存，增加預留庫存
                stockBucket.set(String.valueOf(currentStock - qty));

                int currentReserved = reservedBucket.isExists() ? Integer.parseInt(reservedBucket.get()) : 0;
                reservedBucket.set(String.valueOf(currentReserved + qty));

                // 4. 寫入訂單預留紀錄哈希表
                orderResMap.put("productId", productId);
                orderResMap.put("quantity", String.valueOf(qty));
                orderResMap.put("status", "RESERVED");
                orderResMap.put("updatedAt", String.valueOf(System.currentTimeMillis()));

                // 更新商品可用庫存修改時間
                redissonClient.getBucket("product:" + productId + ":updatedAt", StringCodec.INSTANCE)
                        .set(String.valueOf(System.currentTimeMillis()));

                log.info("[InventoryService] Redisson 庫存預留成功，扣除產品 {} 可用庫存 {} 件，轉為預留狀態",
                        productId, qty);

                // 5. 發布 Axon 事件推動 Saga
                eventGateway.publish(new StockReservedEvent(orderId, productId));

                // 6. 發送 Kafka 訊息以非同步同步到 MySQL
                try {
                    InventorySyncEvent syncEvent = new InventorySyncEvent(
                            orderId,
                            productId,
                            qty,
                            "RESERVE",
                            System.currentTimeMillis()
                    );
                    String json = objectMapper.writeValueAsString(syncEvent);
                    kafkaTemplate.send(InventoryKafkaConfig.INVENTORY_SYNC_TOPIC, productId, json);
                    log.info("[InventoryService] 已發送庫存預留同步訊息至 Kafka: orderId={}, productId={}", orderId, productId);
                } catch (Exception e) {
                    log.error("[InventoryService] 發送 Kafka 同步訊息失敗: {}", e.getMessage(), e);
                }
            } else {
                log.error("[InventoryService] 獲取分散式鎖超時，系統繁忙: lockKey={}", lockKey);
                eventGateway.publish(new StockFailedEvent(orderId, "系統繁忙，請稍後再試"));
            }
        } catch (InterruptedException e) {
            log.error("[InventoryService] 庫存預留執行緒被中斷", e);
            Thread.currentThread().interrupt();
            eventGateway.publish(new StockFailedEvent(orderId, "系統繁忙，請稍後再試"));
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 當訂單確認付款時的事件處理器。
     * 監聽到 OrderPaidEvent 後，透過 Redisson 分散式鎖安全地將狀態改為 COMPLETED 並扣減預留量。
     */
    @EventHandler
    public void on(OrderPaidEvent event) {
        log.info("[InventoryService] 收到訂單付款成功事件 (Redisson)，訂單 ID: {}", event.getOrderId());

        String orderId = event.getOrderId();
        String orderResKey = "order:" + orderId + ":reservation";
        RMap<String, String> orderResMap = redissonClient.getMap(orderResKey, StringCodec.INSTANCE);

        String productId = orderResMap.get("productId");
        // 容錯機制：若 Redis 快取被清空，從 MySQL 還原
        if (productId == null) {
            Optional<AxonStockReservationEntity> reservationOpt = reservationRepository.findById(orderId);
            if (reservationOpt.isPresent()) {
                productId = reservationOpt.get().getProductId();
            }
        }

        if (productId == null) {
            log.warn("[InventoryService] 未找到訂單 {} 的預留紀錄，無法扣減 Redis 預留庫存", orderId);
            return;
        }

        String lockKey = "lock:product:" + productId;
        RLock lock = redissonClient.getLock(lockKey);

        boolean locked = false;
        try {
            locked = lock.tryLock(5, TimeUnit.SECONDS);
            if (locked) {
                String status = orderResMap.get("status");
                if (status == null) {
                    Optional<AxonStockReservationEntity> resOpt = reservationRepository.findById(orderId);
                    status = resOpt.map(AxonStockReservationEntity::getStatus).orElse(null);
                }

                if ("RESERVED".equals(status)) {
                    String reservedKey = "product:" + productId + ":reserved";
                    RBucket<String> reservedBucket = redissonClient.getBucket(reservedKey, StringCodec.INSTANCE);

                    String qtyStr = orderResMap.get("quantity");
                    int qty = qtyStr != null ? Integer.parseInt(qtyStr) : 0;
                    if (qty == 0) {
                        Optional<AxonStockReservationEntity> resOpt = reservationRepository.findById(orderId);
                        qty = resOpt.map(AxonStockReservationEntity::getQuantity).orElse(0);
                    }

                    // 1. 扣減 Redis 預留量
                    int currentReserved = reservedBucket.isExists() ? Integer.parseInt(reservedBucket.get()) : 0;
                    reservedBucket.set(String.valueOf(Math.max(0, currentReserved - qty)));

                    // 2. 更新狀態
                    orderResMap.put("status", "COMPLETED");
                    orderResMap.put("updatedAt", String.valueOf(System.currentTimeMillis()));

                    // 更新商品預留庫存修改時間
                    redissonClient.getBucket("product:" + productId + ":updatedAt", StringCodec.INSTANCE)
                            .set(String.valueOf(System.currentTimeMillis()));

                    log.info("[InventoryService] Redisson 預留庫存扣減成功 (COMMIT): orderId={}", orderId);

                    // 3. 發送 Kafka 同步訊息至 MySQL
                    sendSyncEvent(orderId, productId, qty, "COMMIT");
                } else {
                    log.info("[InventoryService] 訂單 {} 狀態非 RESERVED ({})，忽略 COMMIT", orderId, status);
                }
            }
        } catch (InterruptedException e) {
            log.error("[InventoryService] 扣減預留庫存執行緒被中斷", e);
            Thread.currentThread().interrupt();
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 當訂單被取消時的事件處理器。
     * 監聽到 OrderCancelledEvent 後，加鎖安全地將原先預留的庫存退還至可用庫存中。
     */
    @EventHandler
    public void on(OrderCancelledEvent event) {
        log.info("[InventoryService] 收到訂單取消事件 (Redisson)，訂單 ID: {}, 原因: {}",
                event.getOrderId(), event.getReason());

        String orderId = event.getOrderId();
        String orderResKey = "order:" + orderId + ":reservation";
        RMap<String, String> orderResMap = redissonClient.getMap(orderResKey, StringCodec.INSTANCE);

        String productId = orderResMap.get("productId");
        String status = orderResMap.get("status");

        // 容錯機制
        if (productId == null || status == null) {
            Optional<AxonStockReservationEntity> reservationOpt = reservationRepository.findById(orderId);
            if (reservationOpt.isPresent()) {
                productId = reservationOpt.get().getProductId();
                status = reservationOpt.get().getStatus();
            }
        }

        if (productId == null || status == null) {
            log.warn("[InventoryService] 未找到訂單 {} 的預留記錄，無須執行庫存退回", orderId);
            return;
        }

        String lockKey = "lock:product:" + productId;
        RLock lock = redissonClient.getLock(lockKey);

        boolean locked = false;
        try {
            locked = lock.tryLock(5, TimeUnit.SECONDS);
            if (locked) {
                String qtyStr = orderResMap.get("quantity");
                int qty = qtyStr != null ? Integer.parseInt(qtyStr) : 0;
                if (qty == 0) {
                    Optional<AxonStockReservationEntity> resOpt = reservationRepository.findById(orderId);
                    qty = resOpt.map(AxonStockReservationEntity::getQuantity).orElse(0);
                }

                String stockKey = "product:" + productId + ":stock";
                String reservedKey = "product:" + productId + ":reserved";
                RBucket<String> stockBucket = redissonClient.getBucket(stockKey, StringCodec.INSTANCE);
                RBucket<String> reservedBucket = redissonClient.getBucket(reservedKey, StringCodec.INSTANCE);

                if ("RESERVED".equals(status)) {
                    // 1. 未付款取消 (RELEASE)：將庫存加回可用，並扣減預留量
                    int currentStock = stockBucket.isExists() ? Integer.parseInt(stockBucket.get()) : 0;
                    stockBucket.set(String.valueOf(currentStock + qty));

                    int currentReserved = reservedBucket.isExists() ? Integer.parseInt(reservedBucket.get()) : 0;
                    reservedBucket.set(String.valueOf(Math.max(0, currentReserved - qty)));

                    orderResMap.put("status", "RELEASED");
                    orderResMap.put("updatedAt", String.valueOf(System.currentTimeMillis()));

                    // 更新商品庫存修改時間
                    redissonClient.getBucket("product:" + productId + ":updatedAt", StringCodec.INSTANCE)
                            .set(String.valueOf(System.currentTimeMillis()));
                    log.info("[InventoryService] Redisson 庫存釋放成功 (RELEASE): orderId={}", orderId);

                    // 2. 發送 Kafka 同步訊息至 MySQL
                    sendSyncEvent(orderId, productId, qty, "RELEASE");

                } else if ("COMPLETED".equals(status)) {
                    // 3. 已付款後取消退貨 (REFUND)：僅加回可用庫存
                    int currentStock = stockBucket.isExists() ? Integer.parseInt(stockBucket.get()) : 0;
                    stockBucket.set(String.valueOf(currentStock + qty));

                    orderResMap.put("status", "REFUNDED");
                    orderResMap.put("updatedAt", String.valueOf(System.currentTimeMillis()));

                    // 更新商品庫存修改時間
                    redissonClient.getBucket("product:" + productId + ":updatedAt", StringCodec.INSTANCE)
                            .set(String.valueOf(System.currentTimeMillis()));
                    log.info("[InventoryService] Redisson 已付款庫存釋放成功 (REFUND): orderId={}", orderId);

                    // 4. 發送 Kafka 同步訊息至 MySQL
                    sendSyncEvent(orderId, productId, qty, "REFUND");
                } else {
                    log.info("[InventoryService] 訂單 {} 狀態非 RESERVED/COMPLETED ({})，無須執行庫存釋放", orderId, status);
                }
            }
        } catch (InterruptedException e) {
            log.error("[InventoryService] 釋放庫存執行緒被中斷", e);
            Thread.currentThread().interrupt();
        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void sendSyncEvent(String orderId, String productId, int qty, String action) {
        try {
            InventorySyncEvent syncEvent = new InventorySyncEvent(
                    orderId,
                    productId,
                    qty,
                    action,
                    System.currentTimeMillis()
            );
            String json = objectMapper.writeValueAsString(syncEvent);
            kafkaTemplate.send(InventoryKafkaConfig.INVENTORY_SYNC_TOPIC, productId, json);
        } catch (Exception e) {
            log.error("[InventoryService] 發送 {} 同步訊息失敗: {}", action, e.getMessage(), e);
        }
    }
}
