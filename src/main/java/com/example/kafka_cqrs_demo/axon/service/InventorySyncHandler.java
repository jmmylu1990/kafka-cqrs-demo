package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.dto.InventorySyncEvent;

/**
 * 庫存同步處理策略介面 (Inventory Sync Handler Interface)
 * <p>
 * 定義處理 Kafka 庫存同步事件的合約，落實開放封閉原則 (OCP)。
 * </p>
 */
public interface InventorySyncHandler {

    /**
     * 處理特定的庫存同步事件。
     *
     * @param event 庫存同步事件 DTO
     */
    void handle(InventorySyncEvent event);
}
