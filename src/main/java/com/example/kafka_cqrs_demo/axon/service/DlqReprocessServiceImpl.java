package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.dto.DlqReprocessReport;
import com.example.kafka_cqrs_demo.axon.dto.InventorySyncEvent;
import com.example.kafka_cqrs_demo.axon.repository.AxonDlqMessageRepository;
import com.example.kafka_cqrs_demo.entity.AxonDlqMessageEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * 死信重處理服務實作類別 (DLQ Reprocess Service Implementation)
 */
@Slf4j
@Service
public class DlqReprocessServiceImpl implements DlqReprocessService {

    private final AxonDlqMessageRepository dlqMessageRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public DlqReprocessServiceImpl(AxonDlqMessageRepository dlqMessageRepository,
                                  KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper) {
        this.dlqMessageRepository = dlqMessageRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public DlqReprocessReport reprocess() {
        log.info("[DLQ Reprocess Service] 開始執行死信佇列一鍵重試自癒");
        
        List<AxonDlqMessageEntity> pendingMessages = dlqMessageRepository.findByStatus("PENDING");
        if (pendingMessages.isEmpty()) {
            return new DlqReprocessReport(true, 0, new ArrayList<>(), "無待處理之死信訊息");
        }

        List<String> details = new ArrayList<>();
        int successCount = 0;

        for (AxonDlqMessageEntity entity : pendingMessages) {
            try {
                String messageKey = null;
                String orderIdStr = "未知";
                
                // 嘗試解析 JSON 以提取 productId 作為 Kafka Key (確保分割區順序性)
                try {
                    InventorySyncEvent event = objectMapper.readValue(entity.getMessageContent(), InventorySyncEvent.class);
                    if (event != null) {
                        messageKey = event.getProductId();
                        orderIdStr = event.getOrderId();
                    }
                } catch (Exception parseEx) {
                    log.warn("[DLQ Reprocess] 無法解析死信內容為標準庫存事件，將不使用 Partition Key 發送. ID={}, error={}", 
                            entity.getId(), parseEx.getMessage());
                }

                // 重新發送訊息回原始 Topic
                kafkaTemplate.send(entity.getTopic(), messageKey, entity.getMessageContent());
                
                // 更新資料庫狀態為已處理
                entity.setStatus("REPROCESSED");
                entity.setCreateTime(LocalDateTime.now()); // 更新操作時間
                dlqMessageRepository.save(entity);

                successCount++;
                details.add(String.format("成功重試死信 ID [%s]: orderId=%s, productId=%s, 轉發至 Topic=%s", 
                        entity.getId(), orderIdStr, messageKey != null ? messageKey : "無", entity.getTopic()));

            } catch (Exception ex) {
                log.error("[DLQ Reprocess] 重發死信訊息失敗. ID={}, error={}", entity.getId(), ex.getMessage());
                entity.setStatus("FAILED");
                entity.setErrorMessage(ex.getMessage().length() > 500 ? ex.getMessage().substring(0, 500) : ex.getMessage());
                dlqMessageRepository.save(entity);
                details.add(String.format("重試死信失敗 ID [%s]: %s", entity.getId(), ex.getMessage()));
            }
        }

        return new DlqReprocessReport(true, successCount, details, "死信重處理執行完成");
    }
}
