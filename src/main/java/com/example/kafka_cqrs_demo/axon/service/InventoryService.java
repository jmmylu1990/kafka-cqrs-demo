package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.command.ReserveStockCommand;
import com.example.kafka_cqrs_demo.axon.event.OrderCancelledEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderPaidEvent;
import com.example.kafka_cqrs_demo.axon.event.StockFailedEvent;
import com.example.kafka_cqrs_demo.axon.event.StockReservedEvent;
import com.example.kafka_cqrs_demo.axon.repository.AxonInventoryRepository;
import com.example.kafka_cqrs_demo.axon.repository.AxonStockReservationRepository;
import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
import com.example.kafka_cqrs_demo.entity.AxonStockReservationEntity;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * 庫存服務 (Inventory Service)
 * <p>
 * 負責處理來自 Saga 流程的庫存扣減/預留請求。
 * 本服務接收到 {@link ReserveStockCommand} 指令後，在 MySQL 資料庫中執行實際的可用庫存扣除與預留邏輯，
 * 並發布 StockReservedEvent（成功）或 StockFailedEvent（失敗）事件。
 * 此外，此服務還監聽訂單付款完成與取消事件，用以提交扣庫存或執行庫存釋放退回。
 * </p>
 */
@Slf4j
@Service
public class InventoryService {

    private final EventGateway eventGateway;
    private final AxonInventoryRepository inventoryRepository;
    private final AxonStockReservationRepository reservationRepository;

    /**
     * 庫存服務建構子。
     */
    public InventoryService(EventGateway eventGateway,
                            AxonInventoryRepository inventoryRepository,
                            AxonStockReservationRepository reservationRepository) {
        this.eventGateway = eventGateway;
        this.inventoryRepository = inventoryRepository;
        this.reservationRepository = reservationRepository;
    }

    /**
     * 處理預留庫存指令的 CommandHandler。
     * <p>
     * 業務邏輯：
     * 1. 查詢該商品是否存在，若不存在則發布庫存不足/失敗事件。
     * 2. 比對庫存是否充足。若可用庫存不足，則發布 {@link StockFailedEvent}。
     * 3. 若足夠，扣減可用庫存，累加預留庫存，並寫入 {@link AxonStockReservationEntity} 預留明細，
     *    隨後發布 {@link StockReservedEvent}。
     * </p>
     */
    @CommandHandler
    @Transactional
    public void handle(ReserveStockCommand command) {
        log.info("[InventoryService] 收到庫存預留請求: orderId={}, productId={}, quantity={}",
                command.getOrderId(), command.getProductId(), command.getQuantity());

        Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(command.getProductId());

        if (inventoryOpt.isEmpty()) {
            log.warn("[InventoryService] 預留失敗：商品 {} 不存在", command.getProductId());
            eventGateway.publish(new StockFailedEvent(command.getOrderId(), "商品不存在"));
            return;
        }

        AxonInventoryEntity inventory = inventoryOpt.get();
        if (inventory.getStock() < command.getQuantity()) {
            log.warn("[InventoryService] 預留失敗：商品 {} 庫存不足。剩餘庫存: {}, 請求數量: {}",
                    command.getProductId(), inventory.getStock(), command.getQuantity());
            eventGateway.publish(new StockFailedEvent(command.getOrderId(), "庫存不足"));
            return;
        }

        // 扣減可用庫存，增加預留庫存
        inventory.setStock(inventory.getStock() - command.getQuantity());
        inventory.setReservedStock(inventory.getReservedStock() + command.getQuantity());
        inventoryRepository.save(inventory);

        // 建立並保存預留記錄
        AxonStockReservationEntity reservation = new AxonStockReservationEntity(
                command.getOrderId(),
                command.getProductId(),
                command.getQuantity(),
                "RESERVED",
                LocalDateTime.now()
        );
        reservationRepository.save(reservation);

        log.info("[InventoryService] 庫存預留成功，扣除產品 {} 可用庫存 {} 件，轉為預留狀態",
                command.getProductId(), command.getQuantity());
        eventGateway.publish(new StockReservedEvent(command.getOrderId(), command.getProductId()));
    }

    /**
     * 當訂單確認付款時的事件處理器。
     * 監聽到 OrderPaidEvent 後，確認預留紀錄為已完成，並消去對應的預留庫存。
     */
    @EventHandler
    @Transactional
    public void on(OrderPaidEvent event) {
        log.info("[InventoryService] 收到訂單付款成功事件，確認扣減庫存，訂單 ID: {}", event.getOrderId());

        Optional<AxonStockReservationEntity> reservationOpt = reservationRepository.findById(event.getOrderId());
        if (reservationOpt.isPresent()) {
            AxonStockReservationEntity reservation = reservationOpt.get();
            if ("RESERVED".equals(reservation.getStatus())) {
                // 更新預留狀態為已完成
                reservation.setStatus("COMPLETED");
                reservation.setUpdateTime(LocalDateTime.now());
                reservationRepository.save(reservation);

                // 扣除預留庫存 (因交易已正式成立)
                Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(reservation.getProductId());
                if (inventoryOpt.isPresent()) {
                    AxonInventoryEntity inventory = inventoryOpt.get();
                    inventory.setReservedStock(Math.max(0, inventory.getReservedStock() - reservation.getQuantity()));
                    inventoryRepository.save(inventory);
                    log.info("[InventoryService] 訂單 {} 交易完成，扣減產品 {} 預留庫存 {} 件",
                            event.getOrderId(), reservation.getProductId(), reservation.getQuantity());
                }
            }
        }
    }

    /**
     * 當訂單被取消時的事件處理器。
     * 監聽到 OrderCancelledEvent 後，將原先預留的庫存退還至可用庫存中。
     * - 若為未付款取消 (RESERVED)，釋放預留量並加回可用庫存，狀態設為 RELEASED。
     * - 若為已付款取消 (COMPLETED)，僅加回可用庫存（因預留數已於付款時消去），狀態設為 REFUNDED。
     */
    @EventHandler
    @Transactional
    public void on(OrderCancelledEvent event) {
        log.info("[InventoryService] 收到訂單取消事件，準備釋放預留庫存，訂單 ID: {}, 原因: {}",
                event.getOrderId(), event.getReason());

        Optional<AxonStockReservationEntity> reservationOpt = reservationRepository.findById(event.getOrderId());
        if (reservationOpt.isPresent()) {
            AxonStockReservationEntity reservation = reservationOpt.get();
            String currentStatus = reservation.getStatus();

            if ("RESERVED".equals(currentStatus)) {
                // 1. 未付款取消：更新預留狀態為已釋放 (RELEASED)
                reservation.setStatus("RELEASED");
                reservation.setUpdateTime(LocalDateTime.now());
                reservationRepository.save(reservation);

                // 將預留庫存加回可用庫存，並扣減預留數
                Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(reservation.getProductId());
                if (inventoryOpt.isPresent()) {
                    AxonInventoryEntity inventory = inventoryOpt.get();
                    inventory.setStock(inventory.getStock() + reservation.getQuantity());
                    inventory.setReservedStock(Math.max(0, inventory.getReservedStock() - reservation.getQuantity()));
                    inventoryRepository.save(inventory);
                    log.info("[InventoryService] 成功釋放未付款訂單 {} 的預留量，退回產品 {} 可用庫存 {} 件",
                            event.getOrderId(), reservation.getProductId(), reservation.getQuantity());
                }
            } else if ("COMPLETED".equals(currentStatus)) {
                // 2. 已付款後取消（退貨）：更新預留狀態為已退款 (REFUNDED)
                reservation.setStatus("REFUNDED");
                reservation.setUpdateTime(LocalDateTime.now());
                reservationRepository.save(reservation);

                // 僅將庫存加回可用庫存（預留數在付款時已被消去，因此無須重複扣減）
                Optional<AxonInventoryEntity> inventoryOpt = inventoryRepository.findById(reservation.getProductId());
                if (inventoryOpt.isPresent()) {
                    AxonInventoryEntity inventory = inventoryOpt.get();
                    inventory.setStock(inventory.getStock() + reservation.getQuantity());
                    inventoryRepository.save(inventory);
                    log.info("[InventoryService] 成功釋放已付款訂單 {} 的退貨庫存，退回產品 {} 可用庫存 {} 件",
                            event.getOrderId(), reservation.getProductId(), reservation.getQuantity());
                }
            } else {
                log.info("[InventoryService] 訂單 {} 的預留狀態為 {}，無須執行庫存退回", 
                        event.getOrderId(), currentStatus);
            }
        }
    }
}
