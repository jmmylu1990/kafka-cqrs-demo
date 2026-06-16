package com.example.kafka_cqrs_demo.axon.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 死信重處理執行結果報告 (DLQ Reprocess Report)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DlqReprocessReport {
    private boolean success;
    private int reprocessedCount;
    private List<String> details;
    private String message;
}
