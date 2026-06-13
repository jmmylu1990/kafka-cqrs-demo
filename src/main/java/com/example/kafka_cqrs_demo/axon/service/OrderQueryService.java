package com.example.kafka_cqrs_demo.axon.service;

import java.util.Set;

/**
 * 訂單查詢服務介面 (Order Query Service Interface)
 * <p>
 * 提供唯讀的訂單查詢業務抽象，落實介面隔離與依賴反轉原則。
 * </p>
 */
public interface OrderQueryService {

    /**
     * 根據訂單唯一識別碼查詢訂單詳情。
     *
     * @param orderId 訂單唯一識別碼 (UUID)
     * @return 訂單的 JSON 詳細資料，若找不到則返回「訂單不存在」
     */
    String getOrder(String orderId);

    /**
     * 查詢所有已建立的訂單 ID 列表。
     *
     * @return 包含所有訂單 ID 的 Set 集合
     */
    Set<String> getAllOrderIds();
}
