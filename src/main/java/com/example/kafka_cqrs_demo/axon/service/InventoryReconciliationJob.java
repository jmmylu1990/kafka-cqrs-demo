package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.repository.AxonInventoryRepository;
import com.example.kafka_cqrs_demo.axon.repository.AxonStockReservationRepository;
import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
import com.example.kafka_cqrs_demo.entity.AxonStockReservationEntity;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 庫存與預留記錄對帳任務 (Inventory and Reservation Reconciliation Job)
 * <p>
 * 定期執行對帳機制，比對 MySQL (Source of Truth) 與 Redis 的資料。
 * 若發現 Redis 中的可用庫存、預留庫存或訂單預留明細狀態與 MySQL 不一致，
 * 將自動以 MySQL 資料為準，進行自我修復寫回 Redis。
 * </p>
 */
@Slf4j
@Service
public class InventoryReconciliationJob {

    private final AxonInventoryRepository inventoryRepository;
    private final AxonStockReservationRepository reservationRepository;
    private final RedissonClient redissonClient;

    public InventoryReconciliationJob(AxonInventoryRepository inventoryRepository,
                                    AxonStockReservationRepository reservationRepository,
                                    RedissonClient redissonClient) {
        this.inventoryRepository = inventoryRepository;
        this.reservationRepository = reservationRepository;
        this.redissonClient = redissonClient;
    }

    /**
     * 定期執行對帳，預設為每小時執行一次。
     */
    @Scheduled(cron = "${inventory.reconciliation.cron:0 0 * * * *}")
    public ReconciliationReport runReconciliationScheduled() {
        log.info("[Reconciliation] 定時對帳任務啟動...");
        ReconciliationReport report = reconcile();
        log.info("[Reconciliation] 定時對帳任務結束。稽核商品數: {}, 庫存偏差數: {}, 稽核預留數: {}, 預留偏差數: {}",
                report.getProductsAudited(), report.getStockDriftsDetected(),
                report.getReservationsAudited(), report.getReservationDriftsDetected());
        return report;
    }

    /**
     * 執行對帳與自我修復邏輯。
     */
    public ReconciliationReport reconcile() {
        ReconciliationReport report = new ReconciliationReport();
        report.setTimestamp(LocalDateTime.now().toString());

        int productsAudited = 0;
        int stockDriftsDetected = 0;
        int stockDriftsCorrected = 0;

        int reservationsAudited = 0;
        int reservationDriftsDetected = 0;
        int reservationDriftsCorrected = 0;

        List<String> details = new ArrayList<>();

        // 1. 商品庫存對帳 (MySQL -> Redis)
        List<AxonInventoryEntity> mysqlInventories = inventoryRepository.findAll();
        for (AxonInventoryEntity mysqlInv : mysqlInventories) {
            String productId = mysqlInv.getProductId();
            String lockKey = "{product:" + productId + "}:lock:product";
            RLock lock = redissonClient.getLock(lockKey);
            boolean locked = false;
            try {
                // 嘗試獲取分散式鎖，避免影響正常的交易進行
                locked = lock.tryLock(3, TimeUnit.SECONDS);
                if (locked) {
                    String stockKey = "{product:" + productId + "}:stock";
                    String reservedKey = "{product:" + productId + "}:reserved";
                    RBucket<String> stockBucket = redissonClient.getBucket(stockKey, StringCodec.INSTANCE);
                    RBucket<String> reservedBucket = redissonClient.getBucket(reservedKey, StringCodec.INSTANCE);

                    // 檢查商品最近是否有異動，若在 5 分鐘 (300,000ms) 內，跳過此商品的庫存對帳以防覆寫 Kafka 異步同步中數據
                    RBucket<String> productUpdatedAtBucket = redissonClient.getBucket("{product:" + productId + "}:updatedAt", StringCodec.INSTANCE);
                    String productUpdatedAtStr = productUpdatedAtBucket.get();
                    if (productUpdatedAtStr != null) {
                        try {
                            long pUpdatedAt = Long.parseLong(productUpdatedAtStr);
                            if ((System.currentTimeMillis() - pUpdatedAt) < 300000) {
                                log.info("[Reconciliation] 商品 {} 在 5 分鐘內有交易異動，跳過可用與預留庫存對帳", productId);
                                productsAudited++;
                                continue;
                            }
                        } catch (NumberFormatException e) {
                            // ignore
                        }
                    }

                    int mysqlStock = mysqlInv.getStock();
                    int mysqlReserved = mysqlInv.getReservedStock();

                    boolean stockDrift = false;
                    boolean reservedDrift = false;

                    String redisStockStr = stockBucket.get();
                    if (redisStockStr == null) {
                        stockDrift = true;
                    } else {
                        try {
                            int redisStock = Integer.parseInt(redisStockStr);
                            if (redisStock != mysqlStock) {
                                stockDrift = true;
                            }
                        } catch (NumberFormatException e) {
                            stockDrift = true;
                        }
                    }

                    String redisReservedStr = reservedBucket.get();
                    if (redisReservedStr == null) {
                        reservedDrift = true;
                    } else {
                        try {
                            int redisReserved = Integer.parseInt(redisReservedStr);
                            if (redisReserved != mysqlReserved) {
                                reservedDrift = true;
                            }
                        } catch (NumberFormatException e) {
                            reservedDrift = true;
                        }
                    }

                    if (stockDrift) {
                        stockDriftsDetected++;
                        stockBucket.set(String.valueOf(mysqlStock));
                        stockDriftsCorrected++;
                        String msg = String.format("商品 %s 可用庫存不一致，已修復: Redis=%s, MySQL=%d",
                                productId, redisStockStr, mysqlStock);
                        details.add(msg);
                        log.warn("[Reconciliation] " + msg);
                    }

                    if (reservedDrift) {
                        stockDriftsDetected++;
                        reservedBucket.set(String.valueOf(mysqlReserved));
                        stockDriftsCorrected++;
                        String msg = String.format("商品 %s 預留庫存不一致，已修復: Redis=%s, MySQL=%d",
                                productId, redisReservedStr, mysqlReserved);
                        details.add(msg);
                        log.warn("[Reconciliation] " + msg);
                    }
                    productsAudited++;
                } else {
                    String msg = String.format("跳過商品 %s 的庫存對帳：無法取得分散式鎖", productId);
                    details.add(msg);
                    log.warn("[Reconciliation] " + msg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                String msg = String.format("商品 %s 的庫存對帳過程中斷", productId);
                details.add(msg);
                log.error("[Reconciliation] " + msg, e);
            } finally {
                if (locked && lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }

        // 2. 訂單預留紀錄對帳 (雙向對帳與修復)
        Set<String> auditedOrderIds = new HashSet<>();

        // 2.1 MySQL -> Redis: 檢查所有 MySQL 中狀態為 RESERVED 的預留明細
        List<AxonStockReservationEntity> mysqlReservations = reservationRepository.findAll();
        for (AxonStockReservationEntity mysqlRes : mysqlReservations) {
            if ("RESERVED".equalsIgnoreCase(mysqlRes.getStatus())) {
                String orderId = mysqlRes.getOrderId();
                auditedOrderIds.add(orderId);

                String orderResKey = "order:" + orderId + ":reservation";
                RMap<String, String> orderResMap = redissonClient.getMap(orderResKey, StringCodec.INSTANCE);

                boolean needsCorrection = false;
                String redisProductId = null;
                String redisQty = null;
                String redisStatus = null;

                if (!orderResMap.isExists()) {
                    needsCorrection = true;
                } else {
                    // 檢查訂單預留最近是否有異動，若在 5 分鐘 (300,000ms) 內，跳過以避免覆寫 Kafka 未同步完成之數據
                    String redisUpdatedAtStr = orderResMap.get("updatedAt");
                    if (redisUpdatedAtStr != null) {
                        try {
                            long rUpdatedAt = Long.parseLong(redisUpdatedAtStr);
                            if ((System.currentTimeMillis() - rUpdatedAt) < 300000) {
                                log.info("[Reconciliation] 訂單 {} 在 5 分鐘內有交易異動，跳過 MySQL -> Redis 預留校對", orderId);
                                reservationsAudited++;
                                continue;
                            }
                        } catch (NumberFormatException e) {
                            // ignore
                        }
                    }

                    redisProductId = orderResMap.get("productId");
                    redisQty = orderResMap.get("quantity");
                    redisStatus = orderResMap.get("status");

                    if (!mysqlRes.getProductId().equals(redisProductId) ||
                            !String.valueOf(mysqlRes.getQuantity()).equals(redisQty) ||
                            !"RESERVED".equalsIgnoreCase(redisStatus)) {
                        needsCorrection = true;
                    }
                }

                if (needsCorrection) {
                    reservationDriftsDetected++;
                    orderResMap.put("productId", mysqlRes.getProductId());
                    orderResMap.put("quantity", String.valueOf(mysqlRes.getQuantity()));
                    orderResMap.put("status", "RESERVED");
                    reservationDriftsCorrected++;

                    String msg = String.format("訂單 %s 預留狀態不一致，已修復為 RESERVED: Redis=[productId=%s, qty=%s, status=%s], MySQL=[productId=%s, qty=%d, status=RESERVED]",
                            orderId, redisProductId, redisQty, redisStatus, mysqlRes.getProductId(), mysqlRes.getQuantity());
                    details.add(msg);
                    log.warn("[Reconciliation] " + msg);
                }
                reservationsAudited++;
            }
        }

        // 2.2 Redis -> MySQL: 掃描 Redis 中的預留 Key，檢查是否為孤兒或狀態漂移
        try {
            Iterable<String> redisKeys = redissonClient.getKeys().getKeysByPattern("order:*:reservation");
            for (String key : redisKeys) {
                String orderId = null;
                if (key.startsWith("order:") && key.endsWith(":reservation")) {
                    orderId = key.substring(6, key.length() - 12);
                }

                if (orderId == null || orderId.isEmpty()) {
                    continue;
                }

                // 若在前面的 MySQL -> Redis 步驟已稽核過且確定無誤，則跳過
                if (auditedOrderIds.contains(orderId)) {
                    continue;
                }

                RMap<String, String> orderResMap = redissonClient.getMap(key, StringCodec.INSTANCE);
                String redisProductId = orderResMap.get("productId");
                String redisQty = orderResMap.get("quantity");
                String redisStatus = orderResMap.get("status");
                String redisUpdatedAtStr = orderResMap.get("updatedAt");

                // 檢查訂單預留最近是否有異動，若在 5 分鐘 (300,000ms) 內，跳過以避免覆寫 Kafka 未同步完成之數據
                if (redisUpdatedAtStr != null) {
                    try {
                        long rUpdatedAt = Long.parseLong(redisUpdatedAtStr);
                        if ((System.currentTimeMillis() - rUpdatedAt) < 300000) {
                            log.info("[Reconciliation] 訂單 {} 在 5 分鐘內有交易異動，跳過 Redis -> MySQL 預留校對", orderId);
                            reservationsAudited++;
                            continue;
                        }
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }

                Optional<AxonStockReservationEntity> mysqlResOpt = reservationRepository.findById(orderId);
                if (mysqlResOpt.isEmpty()) {
                    // MySQL 中無此筆預留，判定為 Redis 殘留的孤兒資料，直接刪除
                    reservationDriftsDetected++;
                    orderResMap.delete();
                    reservationDriftsCorrected++;

                    String msg = String.format("刪除 Redis 孤兒預留 Key: %s (MySQL 中無此訂單之預留紀錄)", key);
                    details.add(msg);
                    log.warn("[Reconciliation] " + msg);
                } else {
                    AxonStockReservationEntity mysqlRes = mysqlResOpt.get();
                    boolean statusMatch = mysqlRes.getStatus().equalsIgnoreCase(redisStatus);
                    boolean productMatch = mysqlRes.getProductId().equals(redisProductId);
                    boolean qtyMatch = String.valueOf(mysqlRes.getQuantity()).equals(redisQty);

                    if (!statusMatch || !productMatch || !qtyMatch) {
                        reservationDriftsDetected++;
                        orderResMap.put("productId", mysqlRes.getProductId());
                        orderResMap.put("quantity", String.valueOf(mysqlRes.getQuantity()));
                        orderResMap.put("status", mysqlRes.getStatus());
                        reservationDriftsCorrected++;

                        String msg = String.format("更新 Redis 預留狀態以符合 MySQL: orderId=%s, Redis=[productId=%s, qty=%s, status=%s], MySQL=[productId=%s, qty=%d, status=%s]",
                                orderId, redisProductId, redisQty, redisStatus, mysqlRes.getProductId(), mysqlRes.getQuantity(), mysqlRes.getStatus());
                        details.add(msg);
                        log.warn("[Reconciliation] " + msg);
                    }
                }
                reservationsAudited++;
            }
        } catch (Exception e) {
            log.error("[Reconciliation] 掃描 Redis 預留 Key 失敗", e);
            details.add("掃描 Redis 預留 Key 失敗: " + e.getMessage());
        }

        // 彙整報告
        report.setSuccess(true);
        report.setProductsAudited(productsAudited);
        report.setStockDriftsDetected(stockDriftsDetected);
        report.setStockDriftsCorrected(stockDriftsCorrected);
        report.setReservationsAudited(reservationsAudited);
        report.setReservationDriftsDetected(reservationDriftsDetected);
        report.setReservationDriftsCorrected(reservationDriftsCorrected);
        report.setDetails(details);

        return report;
    }

    /**
     * 對帳報告報告實體 (Reconciliation Report DTO)
     */
    public static class ReconciliationReport {
        private boolean success;
        private int productsAudited;
        private int stockDriftsDetected;
        private int stockDriftsCorrected;
        private int reservationsAudited;
        private int reservationDriftsDetected;
        private int reservationDriftsCorrected;
        private List<String> details;
        private String timestamp;

        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }

        public int getProductsAudited() { return productsAudited; }
        public void setProductsAudited(int productsAudited) { this.productsAudited = productsAudited; }

        public int getStockDriftsDetected() { return stockDriftsDetected; }
        public void setStockDriftsDetected(int stockDriftsDetected) { this.stockDriftsDetected = stockDriftsDetected; }

        public int getStockDriftsCorrected() { return stockDriftsCorrected; }
        public void setStockDriftsCorrected(int stockDriftsCorrected) { this.stockDriftsCorrected = stockDriftsCorrected; }

        public int getReservationsAudited() { return reservationsAudited; }
        public void setReservationsAudited(int reservationsAudited) { this.reservationsAudited = reservationsAudited; }

        public int getReservationDriftsDetected() { return reservationDriftsDetected; }
        public void setReservationDriftsDetected(int reservationDriftsDetected) { this.reservationDriftsDetected = reservationDriftsDetected; }

        public int getReservationDriftsCorrected() { return reservationDriftsCorrected; }
        public void setReservationDriftsCorrected(int reservationDriftsCorrected) { this.reservationDriftsCorrected = reservationDriftsCorrected; }

        public List<String> getDetails() { return details; }
        public void setDetails(List<String> details) { this.details = details; }

        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    }
}
