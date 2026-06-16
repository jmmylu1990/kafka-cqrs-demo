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
import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
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
import org.redisson.api.RScript;
import org.redisson.client.codec.StringCodec;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
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

    private static final String RESERVE_LUA =
            "-- 傳入商品相關的 Redis 鍵\n" +
            "local stockKey = KEYS[1]      -- 可用庫存鍵\n" +
            "local reservedKey = KEYS[2]   -- 預留庫存鍵\n" +
            "local isHotKey = KEYS[3]      -- 冷熱標記鍵\n" +
            "local qty = tonumber(ARGV[1])  -- 欲預留扣減之數量\n" +
            "\n" +
            "-- 1. 檢查可用庫存鍵是否存在 (判斷是否已加載至 Redis 快取)\n" +
            "local stockExists = redis.call('exists', stockKey)\n" +
            "if stockExists == 0 then\n" +
            "    return -1   -- 快取未命中，需回退至 Java 端獲取鎖從 MySQL DCL 加載\n" +
            "end\n" +
            "\n" +
            "-- 2. 獲取當前可用庫存並進行防禦性檢查\n" +
            "local currentStock = tonumber(redis.call('get', stockKey))\n" +
            "if currentStock < qty then\n" +
            "    return 0    -- 可用庫存不足，預留失敗 (Fail-fast)\n" +
            "end\n" +
            "\n" +
            "-- 3. 計算並設定合適的 TTL (防止過期失效導致快取永久化)\n" +
            "local ttl = redis.call('ttl', stockKey)\n" +
            "if ttl < 0 then\n" +
            "    local isHot = redis.call('get', isHotKey)\n" +
            "    if isHot == 'true' then\n" +
            "        ttl = 86400  -- 熱商品 TTL 預設 1 天\n" +
            "    else\n" +
            "        ttl = 60     -- 冷商品 TTL 預設 60 秒\n" +
            "    end\n" +
            "end\n" +
            "\n" +
            "-- 4. 執行可用庫存扣減與預留庫存累加\n" +
            "local newStock = currentStock - qty\n" +
            "local currentReserved = tonumber(redis.call('get', reservedKey) or '0')\n" +
            "local newReserved = currentReserved + qty\n" +
            "\n" +
            "-- 5. 寫回 Redis 並刷新 TTL，保證原子變更\n" +
            "redis.call('setex', stockKey, ttl, tostring(newStock))\n" +
            "redis.call('setex', reservedKey, ttl, tostring(newReserved))\n" +
            "return 1; -- 預留成功";

    private static final String COMMIT_LUA =
            "-- 傳入預留庫存鍵與冷熱標記鍵\n" +
            "local reservedKey = KEYS[1]   -- 預留庫存鍵\n" +
            "local isHotKey = KEYS[2]      -- 冷熱標記鍵\n" +
            "local qty = tonumber(ARGV[1])  -- 付款成功需確認扣減之數量\n" +
            "\n" +
            "-- 1. 獲取並扣減預留庫存，防止負值\n" +
            "local currentReserved = tonumber(redis.call('get', reservedKey) or '0')\n" +
            "local newReserved = currentReserved - qty\n" +
            "if newReserved < 0 then\n" +
            "    newReserved = 0\n" +
            "end\n" +
            "\n" +
            "-- 2. 計算合適的 TTL\n" +
            "local ttl = redis.call('ttl', reservedKey)\n" +
            "if ttl < 0 then\n" +
            "    local isHot = redis.call('get', isHotKey)\n" +
            "    if isHot == 'true' then\n" +
            "        ttl = 86400  -- 熱商品 1 天\n" +
            "    else\n" +
            "        ttl = 60     -- 冷商品 60 秒\n" +
            "    end\n" +
            "end\n" +
            "\n" +
            "-- 3. 寫回預留庫存並刷新 TTL\n" +
            "redis.call('setex', reservedKey, ttl, tostring(newReserved))\n" +
            "return 1; -- 確認成功";

    private static final String RELEASE_LUA =
            "-- 傳入可用庫存鍵、預留庫存鍵與冷熱標記鍵\n" +
            "local stockKey = KEYS[1]      -- 可用庫存鍵\n" +
            "local reservedKey = KEYS[2]   -- 預留庫存鍵\n" +
            "local isHotKey = KEYS[3]      -- 冷熱標記鍵\n" +
            "local qty = tonumber(ARGV[1])  -- 取消訂單需釋放退回之數量\n" +
            "\n" +
            "-- 1. 讀取可用庫存與預留庫存，並將數量加回可用、從預留扣除 (防負數)\n" +
            "local currentStock = tonumber(redis.call('get', stockKey) or '0')\n" +
            "local currentReserved = tonumber(redis.call('get', reservedKey) or '0')\n" +
            "\n" +
            "local newStock = currentStock + qty\n" +
            "local newReserved = currentReserved - qty\n" +
            "if newReserved < 0 then\n" +
            "    newReserved = 0\n" +
            "end\n" +
            "\n" +
            "-- 2. 計算 TTL\n" +
            "local ttl = redis.call('ttl', stockKey)\n" +
            "if ttl < 0 then\n" +
            "    local isHot = redis.call('get', isHotKey)\n" +
            "    if isHot == 'true' then\n" +
            "        ttl = 86400  -- 熱商品 1 天\n" +
            "    else\n" +
            "        ttl = 60     -- 冷商品 60 秒\n" +
            "    end\n" +
            "end\n" +
            "\n" +
            "-- 3. 寫回 Redis 並刷新 TTL\n" +
            "redis.call('setex', stockKey, ttl, tostring(newStock))\n" +
            "redis.call('setex', reservedKey, ttl, tostring(newReserved))\n" +
            "return 1; -- 釋放成功";

    private static final String REFUND_LUA =
            "-- 傳入可用庫存鍵與冷熱標記鍵\n" +
            "local stockKey = KEYS[1]      -- 可用庫存鍵\n" +
            "local isHotKey = KEYS[2]      -- 冷熱標記鍵\n" +
            "local qty = tonumber(ARGV[1])  -- 退貨需退回可用庫存之數量\n" +
            "\n" +
            "-- 1. 讀取當前可用庫存並加回退貨數量\n" +
            "local currentStock = tonumber(redis.call('get', stockKey) or '0')\n" +
            "local newStock = currentStock + qty\n" +
            "\n" +
            "-- 2. 計算 TTL\n" +
            "local ttl = redis.call('ttl', stockKey)\n" +
            "if ttl < 0 then\n" +
            "    local isHot = redis.call('get', isHotKey)\n" +
            "    if isHot == 'true' then\n" +
            "        ttl = 86400  -- 熱商品 1 天\n" +
            "    else\n" +
            "        ttl = 60     -- 冷商品 60 秒\n" +
            "    end\n" +
            "end\n" +
            "\n" +
            "-- 3. 寫回可用庫存並刷新 TTL\n" +
            "redis.call('setex', stockKey, ttl, tostring(newStock))\n" +
            "return 1; -- 退款成功";

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
        log.info("[InventoryService] 收到庫存預留請求 (Lua): orderId={}, productId={}, quantity={}",
                command.getOrderId(), command.getProductId(), command.getQuantity());

        String productId = command.getProductId();
        String orderId = command.getOrderId();
        int qty = command.getQuantity();

        String stockKey = "{product:" + productId + "}:stock";
        String reservedKey = "{product:" + productId + "}:reserved";
        String isHotKey = "{product:" + productId + "}:isHot";
        String orderResKey = "order:" + orderId + ":reservation";

        List<Object> keys = Arrays.asList(stockKey, reservedKey, isHotKey);

        // 1. 執行 Lua 腳本嘗試預留庫存
        Long result = redissonClient.getScript(StringCodec.INSTANCE)
                .eval(RScript.Mode.READ_WRITE, RESERVE_LUA, RScript.ReturnType.INTEGER, keys, qty);

        // 2. 若快取未命中 (result == -1)，則獲取臨時加載鎖，並從 MySQL 載入至 Redis 快取
        if (result == -1) {
            String loadLockKey = "{product:" + productId + "}:lock:load";
            RLock loadLock = redissonClient.getLock(loadLockKey);
            boolean locked = false;
            try {
                // 嘗試獲取載入鎖，最多等待 5 秒
                locked = loadLock.tryLock(5, TimeUnit.SECONDS);
                if (locked) {
                    RBucket<String> stockBucket = redissonClient.getBucket(stockKey, StringCodec.INSTANCE);
                    if (!stockBucket.isExists()) {
                        log.info("[InventoryService] Redis 中找不到商品 {} 的庫存，嘗試從 MySQL 載入 (DCL)", productId);
                        Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(productId);
                        if (inventoryOpt.isPresent()) {
                            AxonInventoryEntity dbInventory = inventoryOpt.get();
                            boolean isHot = dbInventory.isHot();
                            long ttl = isHot ? 1 : 60;
                            TimeUnit timeUnit = isHot ? TimeUnit.DAYS : TimeUnit.SECONDS;

                            stockBucket.set(String.valueOf(dbInventory.getStock()), ttl, timeUnit);
                            redissonClient.getBucket(reservedKey, StringCodec.INSTANCE)
                                    .set(String.valueOf(dbInventory.getReservedStock()), ttl, timeUnit);
                            redissonClient.getBucket(isHotKey, StringCodec.INSTANCE)
                                    .set(isHot ? "true" : "false", ttl, timeUnit);

                            log.info("[InventoryService] 成功從 MySQL 載入商品 {} 的庫存至 Redis (庫存: {}, 預留: {})",
                                    productId, dbInventory.getStock(), dbInventory.getReservedStock());
                        } else {
                            log.warn("[InventoryService] 預留失敗：商品 {} 在 MySQL 與 Redis 中皆不存在", productId);
                            eventGateway.publish(new StockFailedEvent(orderId, "商品不存在"));
                            return;
                        }
                    }
                } else {
                    log.error("[InventoryService] 獲取載入鎖超時，系統繁忙: lockKey={}", loadLockKey);
                    eventGateway.publish(new StockFailedEvent(orderId, "系統繁忙，請稍後再試"));
                    return;
                }
            } catch (InterruptedException e) {
                log.error("[InventoryService] 載入庫存執行緒被中斷", e);
                Thread.currentThread().interrupt();
                eventGateway.publish(new StockFailedEvent(orderId, "系統繁忙，請稍後再試"));
                return;
            } finally {
                if (locked && loadLock.isHeldByCurrentThread()) {
                    loadLock.unlock();
                }
            }

            // 再次執行 Lua 腳本進行預留
            result = redissonClient.getScript(StringCodec.INSTANCE)
                    .eval(RScript.Mode.READ_WRITE, RESERVE_LUA, RScript.ReturnType.INTEGER, keys, qty);
        }

        // 3. 處理 Lua 腳本的回傳結果
        if (result == 1) {
            // 預留成功，寫入訂單預留紀錄哈希表
            RMap<String, String> orderResMap = redissonClient.getMap(orderResKey, StringCodec.INSTANCE);
            orderResMap.put("productId", productId);
            orderResMap.put("quantity", String.valueOf(qty));
            orderResMap.put("status", "RESERVED");
            orderResMap.put("updatedAt", String.valueOf(System.currentTimeMillis()));

            // 更新商品可用庫存修改時間
            redissonClient.getBucket("{product:" + productId + "}:updatedAt", StringCodec.INSTANCE)
                    .set(String.valueOf(System.currentTimeMillis()));

            log.info("[InventoryService] Lua 庫存預留成功，扣除產品 {} 可用庫存 {} 件，轉為預留狀態", productId, qty);

            // 發布 Axon 事件推動 Saga
            eventGateway.publish(new StockReservedEvent(orderId, productId));

            // 發送 Kafka 訊息以非同步同步到 MySQL
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
        } else if (result == 0) {
            log.warn("[InventoryService] 預留失敗：商品 {} 庫存不足", productId);
            eventGateway.publish(new StockFailedEvent(orderId, "庫存不足"));
        } else {
            log.warn("[InventoryService] 預留失敗：商品不存在或未明錯誤, result={}", result);
            eventGateway.publish(new StockFailedEvent(orderId, "商品不存在"));
        }
    }

    /**
     * 當訂單確認付款時的事件處理器。
     * 監聽到 OrderPaidEvent 後，透過 Redisson 分散式鎖安全地將狀態改為 COMPLETED 並扣減預留量。
     */
    @EventHandler
    public void on(OrderPaidEvent event) {
        log.info("[InventoryService] 收到訂單付款成功事件 (Lua)，訂單 ID: {}", event.getOrderId());

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

        String status = orderResMap.get("status");
        if (status == null) {
            Optional<AxonStockReservationEntity> resOpt = reservationRepository.findById(orderId);
            status = resOpt.map(AxonStockReservationEntity::getStatus).orElse(null);
        }

        if ("RESERVED".equals(status)) {
            String reservedKey = "{product:" + productId + "}:reserved";
            String isHotKey = "{product:" + productId + "}:isHot";

            String qtyStr = orderResMap.get("quantity");
            int qty = qtyStr != null ? Integer.parseInt(qtyStr) : 0;
            if (qty == 0) {
                Optional<AxonStockReservationEntity> resOpt = reservationRepository.findById(orderId);
                qty = resOpt.map(AxonStockReservationEntity::getQuantity).orElse(0);
            }

            // 1. 執行 Lua 腳本扣減預留量
            List<Object> keys = Arrays.asList(reservedKey, isHotKey);
            redissonClient.getScript(StringCodec.INSTANCE)
                    .eval(RScript.Mode.READ_WRITE, COMMIT_LUA, RScript.ReturnType.INTEGER, keys, qty);

            // 2. 更新狀態
            orderResMap.put("status", "COMPLETED");
            orderResMap.put("updatedAt", String.valueOf(System.currentTimeMillis()));

            // 更新商品預留庫存修改時間
            redissonClient.getBucket("{product:" + productId + "}:updatedAt", StringCodec.INSTANCE)
                    .set(String.valueOf(System.currentTimeMillis()));

            log.info("[InventoryService] Redisson (Lua) 預留庫存扣減成功 (COMMIT): orderId={}", orderId);

            // 3. 發送 Kafka 同步訊息至 MySQL
            sendSyncEvent(orderId, productId, qty, "COMMIT");
        } else {
            log.info("[InventoryService] 訂單 {} 狀態非 RESERVED ({})，忽略 COMMIT", orderId, status);
        }
    }

    /**
     * 當訂單被取消時的事件處理器。
     * 監聽到 OrderCancelledEvent 後，加鎖安全地將原先預留的庫存退還至可用庫存中。
     */
    @EventHandler
    public void on(OrderCancelledEvent event) {
        log.info("[InventoryService] 收到訂單取消事件 (Lua)，訂單 ID: {}, 原因: {}",
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

        String qtyStr = orderResMap.get("quantity");
        int qty = qtyStr != null ? Integer.parseInt(qtyStr) : 0;
        if (qty == 0) {
            Optional<AxonStockReservationEntity> resOpt = reservationRepository.findById(orderId);
            qty = resOpt.map(AxonStockReservationEntity::getQuantity).orElse(0);
        }

        String stockKey = "{product:" + productId + "}:stock";
        String reservedKey = "{product:" + productId + "}:reserved";
        String isHotKey = "{product:" + productId + "}:isHot";

        if ("RESERVED".equals(status)) {
            // 1. 未付款取消 (RELEASE)：將庫存加回可用，並扣減預留量
            List<Object> keys = Arrays.asList(stockKey, reservedKey, isHotKey);
            redissonClient.getScript(StringCodec.INSTANCE)
                    .eval(RScript.Mode.READ_WRITE, RELEASE_LUA, RScript.ReturnType.INTEGER, keys, qty);

            orderResMap.put("status", "RELEASED");
            orderResMap.put("updatedAt", String.valueOf(System.currentTimeMillis()));

            // 更新商品庫存修改時間
            redissonClient.getBucket("{product:" + productId + "}:updatedAt", StringCodec.INSTANCE)
                    .set(String.valueOf(System.currentTimeMillis()));
            log.info("[InventoryService] Redisson (Lua) 庫存釋放成功 (RELEASE): orderId={}", orderId);

            // 2. 發送 Kafka 同步訊息至 MySQL
            sendSyncEvent(orderId, productId, qty, "RELEASE");

        } else if ("COMPLETED".equals(status)) {
            // 3. 已付款後取消退貨 (REFUND)：僅加回可用庫存
            List<Object> keys = Arrays.asList(stockKey, isHotKey);
            redissonClient.getScript(StringCodec.INSTANCE)
                    .eval(RScript.Mode.READ_WRITE, REFUND_LUA, RScript.ReturnType.INTEGER, keys, qty);

            orderResMap.put("status", "REFUNDED");
            orderResMap.put("updatedAt", String.valueOf(System.currentTimeMillis()));

            // 更新商品庫存修改時間
            redissonClient.getBucket("{product:" + productId + "}:updatedAt", StringCodec.INSTANCE)
                    .set(String.valueOf(System.currentTimeMillis()));
            log.info("[InventoryService] Redisson (Lua) 已付款庫存釋放成功 (REFUND): orderId={}", orderId);

            // 4. 發送 Kafka 同步訊息至 MySQL
            sendSyncEvent(orderId, productId, qty, "REFUND");
        } else {
            log.info("[InventoryService] 訂單 {} 狀態非 RESERVED/COMPLETED ({})，無須執行庫存釋放", orderId, status);
        }
    }

    private void updateProductCache(String productId, Integer newStock, Integer newReserved) {
        String isHotKey = "{product:" + productId + "}:isHot";
        RBucket<String> isHotBucket = redissonClient.getBucket(isHotKey, StringCodec.INSTANCE);
        
        String isHotVal = isHotBucket.get();
        boolean isHot = "true".equals(isHotVal);
        long ttl = isHot ? 1 : 60;
        TimeUnit timeUnit = isHot ? TimeUnit.DAYS : TimeUnit.SECONDS;

        if (newStock != null) {
            String stockKey = "{product:" + productId + "}:stock";
            RBucket<String> stockBucket = redissonClient.getBucket(stockKey, StringCodec.INSTANCE);
            stockBucket.set(String.valueOf(newStock), ttl, timeUnit);
        }
        if (newReserved != null) {
            String reservedKey = "{product:" + productId + "}:reserved";
            RBucket<String> reservedBucket = redissonClient.getBucket(reservedKey, StringCodec.INSTANCE);
            reservedBucket.set(String.valueOf(newReserved), ttl, timeUnit);
        }
        isHotBucket.set(isHot ? "true" : "false", ttl, timeUnit); // 刷新 isHot 標記的 TTL
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
