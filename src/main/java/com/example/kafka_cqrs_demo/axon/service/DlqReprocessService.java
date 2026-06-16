package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.dto.DlqReprocessReport;

/**
 * 死信重處理服務介面 (DLQ Reprocess Service Interface)
 */
public interface DlqReprocessService {
    /**
     * 一鍵重試所有狀態為 PENDING 的死信訊息。
     *
     * @return 包含執行細節與成功數量的報告
     */
    DlqReprocessReport reprocess();
}
