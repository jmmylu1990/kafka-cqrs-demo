package com.example.kafka_cqrs_demo.axon.controller;

import com.example.kafka_cqrs_demo.axon.service.InventoryReconciliationJob;
import com.example.kafka_cqrs_demo.axon.service.InventoryReconciliationJob.ReconciliationReport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 庫存對帳控制器 (Inventory Reconciliation Controller)
 * <p>
 * 提供手動觸發庫存與預留記錄對帳機制的第一入口。
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("/axonsaga/api/inventory")
public class InventoryReconciliationController {

    private final InventoryReconciliationJob reconciliationJob;

    public InventoryReconciliationController(InventoryReconciliationJob reconciliationJob) {
        this.reconciliationJob = reconciliationJob;
    }

    /**
     * 手動執行庫存與預留記錄對帳。
     *
     * @return 包含對帳與修復明細的 JSON 報告
     */
    @PostMapping("/reconcile")
    public ReconciliationReport reconcile() {
        log.info("手動觸發庫存對帳與自癒程序");
        return reconciliationJob.reconcile();
    }
}
