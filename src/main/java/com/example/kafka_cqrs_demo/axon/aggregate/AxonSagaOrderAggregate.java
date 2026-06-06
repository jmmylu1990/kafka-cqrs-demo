package com.example.kafka_cqrs_demo.axon.aggregate;

import com.example.kafka_cqrs_demo.axon.command.AxonSagaCreateOrderCommand;
import com.example.kafka_cqrs_demo.axon.command.CancelOrderCommand;
import com.example.kafka_cqrs_demo.axon.command.ConfirmPaymentCommand;
import com.example.kafka_cqrs_demo.axon.command.ConfirmStockReservedCommand;
import com.example.kafka_cqrs_demo.axon.enums.OrderStatus;
import com.example.kafka_cqrs_demo.axon.event.OrderCancelledEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderCreatedEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderPaidEvent;
import com.example.kafka_cqrs_demo.axon.event.OrderStockReservedEvent;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.spring.stereotype.Aggregate;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

/**
 * 訂單領域聚合根 (Order Aggregate)
 * <p>
 * 聚合根是領域驅動設計 (DDD, Domain-Driven Design) 的核心邊界。
 * 所有關於「訂單」的業務變更、狀態轉移都必須透過此聚合根進行。
 * 它負責封裝訂單的內部狀態，執行業務規則與防禦性檢查，並透過發布領域事件 (Domain Events)
 * 來驅動系統其他部分的狀態變更（例如 CQRS 讀取模型更新與 Saga 流程）。
 * </p>
 */
@Slf4j
@Aggregate
public class AxonSagaOrderAggregate {

    /**
     * 聚合根唯一識別碼 (Aggregate Identifier)
     * 作為事件儲存 (Event Store) 中的聚合索引鍵，用以關聯與該訂單相關的所有歷史事件。
     */
    @AggregateIdentifier
    private String orderId;

    /** 訂單所購買的產品識別碼 */
    private String productId;

    /** 訂單購買的產品數量 */
    private int quantity;

    /** 訂單的商品單價，單位為貨幣最小單位（例如分或元） */
    private long price;

    /** 訂單取消或變更的詳細原因說明 */
    private String reason;

    /**
     * 訂單目前的生命週期狀態。
     * 狀態的變更僅能在 EventSourcingHandler 中進行賦值，以保證事件重放時的一致性。
     */
    private OrderStatus status;

    /**
     * 無參數建構子 (No-Args Constructor)。
     * 這是 Axon Framework 內部技術機制的必要需求。
     * 當 Axon 需要從事件儲存 (Event Store) 中重新載入並重放歷史事件以恢復 Aggregate 狀態時，
     * 會先透過此無參數建構子建立一個全新的空執行個體，接著依序呼叫 EventSourcingHandler 方法。
     */
    protected AxonSagaOrderAggregate() {}

    /**
     * 建立訂單的指令處理器 (CommandHandler)。
     * 接收建立訂單的意圖指令，並在狀態正式建立前執行必要的業務規則檢查（例如價格不可為負數）。
     * 當檢查通過後，會發布 OrderCreatedEvent。
     *
     * @param command 包含訂單詳細規格的建立指令
     * @throws IllegalArgumentException 當輸入參數不合法時拋出
     */
    @CommandHandler
    public AxonSagaOrderAggregate(AxonSagaCreateOrderCommand command) {
        log.info("DEBUG: Aggregate 接收到建立指令: {}", command.getOrderId());
        if (command.getPrice() < 0) {
            throw new IllegalArgumentException("價格不能為負數");
        }

        // 發布事件：此動作會將事件持久化至 Event Store，隨後觸發對應的 EventSourcingHandler
        apply(new OrderCreatedEvent(
            command.getOrderId(),
            command.getProductId(),
            command.getQuantity(),
            command.getPrice()
        ));
    }

    /**
     * 付款確認指令處理器 (CommandHandler)。
     * 處理來自 Saga 流程或外部付款系統的付款確認請求。
     *
     * 狀態轉移規則：
     * 1. 為了確保業務流程的順序與嚴謹性，訂單必須先完成「庫存扣減確認」階段（即狀態為 PENDING_PAYMENT）。
     * 2. 若訂單狀態不為 PENDING_PAYMENT，表示該訂單尚未預扣庫存，或是處於已付款、已取消等狀態。
     * 3. 若狀態檢查不通過，將會拋出例外以拒絕此付款指令，防止重複付款或無效付款。
     *
     * @param command 包含付款確認資訊的指令
     * @throws IllegalStateException 當訂單狀態不符合付款條件時拋出
     */
    @CommandHandler
    public void handle(ConfirmPaymentCommand command) {
        log.info("處理付款指令: {}", command.getOrderId());

        // 防禦性檢查：驗證訂單是否處於可付款的狀態（必須是庫存已預留的待付款狀態）
        boolean isEligibleForPayment = (this.status == OrderStatus.PENDING_PAYMENT);

        if (!isEligibleForPayment) {
            log.error("付款失敗：訂單 {} 當前狀態 {} 不允許執行付款", this.orderId, this.status);
            throw new IllegalStateException("訂單目前狀態不允許付款: " + this.status);
        }

        // 驗證通過，套用並發布訂單付款成功事件
        apply(new OrderPaidEvent(this.orderId, "已付款"));
    }

    /**
     * 取消訂單指令處理器 (CommandHandler)。
     * 處理因庫存不足、付款逾時或使用者手動發起的取消訂單要求。
     *
     * 業務規則：
     * 1. 若訂單已經進入「已出貨 (SHIPPED)」或「已送達 (DELIVERED)」狀態，則無法再執行取消。
     * 2. 通過檢查後，套用 OrderCancelledEvent 並將取消原因記錄於事件中。
     *
     * @param command 包含取消訂單識別碼及原因的指令
     * @throws IllegalStateException 當訂單已出貨或送達時拋出
     */
    @CommandHandler
    public void handle(CancelOrderCommand command) {
        if (this.status == OrderStatus.SHIPPED || this.status == OrderStatus.DELIVERED) {
            throw new IllegalStateException("已出貨或送達的訂單無法取消");
        }
        // 套用取消事件，傳遞訂單 ID 與取消原因
        apply(new OrderCancelledEvent(command.getOrderId(), command.getReason()));
    }

    /**
     * 確認庫存預留指令處理器 (CommandHandler)。
     * 當 Inventory 服務成功鎖定商品庫存時，Saga 協調器會發送此指令通知 Order 聚合。
     *
     * @param command 包含訂單 ID 的確認庫存預留指令
     */
    @CommandHandler
    public void handle(ConfirmStockReservedCommand command) {
        log.info("處理確認庫存保留指令: {}", command.getOrderId());
        apply(new OrderStockReservedEvent(command.getOrderId()));
    }

    // =========================================================================
    // 事件來源處理器 (Event Sourcing Handlers)
    // =========================================================================
    // 注意：此區域的方法「絕不能」包含任何業務邏輯、防禦性檢查或呼叫 apply()。
    // 它們的唯一職責是根據已發布的歷史事件，將對應的屬性欄位更新為正確的值。
    // 這確保了在進行事件重放時，聚合能百分之百還原至一致的內部狀態。
    // =========================================================================

    /**
     * 當 OrderCreatedEvent 被套用或重放時觸發。
     * 初始化訂單的基本屬性，並將訂單狀態設為 CREATED。
     *
     * @param event 訂單建立事件
     */
    @EventSourcingHandler
    public void on(OrderCreatedEvent event) {
        log.info("[EventSourcingHandler] 訂單建立，初始化狀態: {}", event.getOrderId());

        this.orderId = event.getOrderId();
        this.productId = event.getProductId();
        this.quantity = event.getQuantity();
        this.price = event.getPrice();
        this.status = OrderStatus.CREATED;
    }

    /**
     * 當 OrderPaidEvent 被套用或重放時觸發。
     * 將訂單狀態更新為 PAID。
     *
     * @param event 訂單付款成功事件
     */
    @EventSourcingHandler
    public void on(OrderPaidEvent event) {
        log.info("[EventSourcingHandler] 訂單已付款: {}", this.orderId);
        this.status = OrderStatus.PAID;
    }

    /**
     * 當 OrderCancelledEvent 被套用或重放時觸發。
     * 將訂單狀態更新為 CANCELLED，並記錄取消原因。
     *
     * @param event 訂單取消事件
     */
    @EventSourcingHandler
    public void on(OrderCancelledEvent event) {
        this.status = OrderStatus.CANCELLED;
        this.reason = event.getReason();
        log.info("[EventSourcingHandler] 訂單已取消，原因: {}", event.getReason());
    }

    /**
     * 當 OrderStockReservedEvent 被套用或重放時觸發。
     * 將訂單狀態更新為 PENDING_PAYMENT。
     * 這代表該訂單已順利鎖定庫存，可以開始進行付款流程。
     *
     * @param event 訂單庫存預留成功事件
     */
    @EventSourcingHandler
    public void on(OrderStockReservedEvent event) {
        this.status = OrderStatus.PENDING_PAYMENT;
        log.info("[EventSourcingHandler] 庫存預留成功，訂單狀態變更為 PENDING_PAYMENT");
    }
}


}