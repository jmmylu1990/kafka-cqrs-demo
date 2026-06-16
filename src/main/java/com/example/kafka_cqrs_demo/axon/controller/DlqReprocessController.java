package com.example.kafka_cqrs_demo.axon.controller;

import com.example.kafka_cqrs_demo.axon.dto.DlqReprocessReport;
import com.example.kafka_cqrs_demo.axon.service.DlqReprocessService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 死信佇列重處理控制器 (DLQ Reprocess Controller)
 * <p>
 * 重構後已符合 SOLID 原則，將資料庫檢索、JSON 解析、Kafka 重新投遞與狀態更新等業務邏輯
 * 抽離至獨立的 DlqReprocessService 中。
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("/axonsaga/api/dlq")
public class DlqReprocessController {

    private final DlqReprocessService dlqReprocessService;

    public DlqReprocessController(DlqReprocessService dlqReprocessService) {
        this.dlqReprocessService = dlqReprocessService;
    }

    /**
     * 一鍵重試所有待處理 (PENDING) 的死信訊息。
     *
     * @return 重試處理報告
     */
    @PostMapping("/reprocess")
    public DlqReprocessReport reprocess() {
        log.info("手動觸發死信佇列一鍵重試自癒程序");
        return dlqReprocessService.reprocess();
    }
}
