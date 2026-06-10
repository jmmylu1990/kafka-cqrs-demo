package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.event.WalletRefundedEvent;
import com.example.kafka_cqrs_demo.axon.repository.AxonRefundRetryTaskRepository;
import com.example.kafka_cqrs_demo.entity.AxonRefundRetryTaskEntity;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 售後退款自癒背景排程器 (Refund Retry Scheduler)
 * <p>
 * 定期掃描 MySQL 資料表 t_payment_refund_retry_task 中狀態為 PENDING 且已達執行時間點的任務。
 * 重新向外部金流 API 發送退款請求，並採用指數退避算法計算下一次重試時間。
 * 重試上限為 5 次，若 5 次皆失敗則進行高優先級警報。
 * </p>
 */
@Slf4j
@Component
@EnableScheduling
public class RefundRetryScheduler {

    private final AxonRefundRetryTaskRepository refundRetryTaskRepository;
    private final EventGateway eventGateway;
    private final RestTemplate restTemplate;

    private static final String REFUND_URL = "http://localhost:8081/api/external/payment/refund";
    private static final int MAX_RETRY_LIMIT = 5;

    public RefundRetryScheduler(AxonRefundRetryTaskRepository refundRetryTaskRepository,
                                EventGateway eventGateway) {
        this.refundRetryTaskRepository = refundRetryTaskRepository;
        this.eventGateway = eventGateway;

        // 配置超時防護的 RestTemplate
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);
        factory.setReadTimeout(3000);
        this.restTemplate = new RestTemplate(factory);
    }

    /**
     * 定期掃描待處理的退款重試任務，每 10 秒執行一次
     */
    @Scheduled(fixedDelay = 10000)
    public void retryPendingRefunds() {
        LocalDateTime now = LocalDateTime.now();
        List<AxonRefundRetryTaskEntity> pendingTasks = refundRetryTaskRepository
                .findByStatusAndNextRetryTimeBefore("PENDING", now);

        if (pendingTasks.isEmpty()) {
            return;
        }

        log.info("[RefundRetryScheduler] 掃描到 {} 筆待執行的退款重試任務，開始批次重試自癒...", pendingTasks.size());

        for (AxonRefundRetryTaskEntity task : pendingTasks) {
            log.info("[RefundRetryScheduler] 開始執行退款重試：taskId={}, orderId={}, 當前已重試次數={}",
                    task.getId(), task.getOrderId(), task.getRetryCount());

            PaymentApiRequest apiRequest = new PaymentApiRequest(task.getUserId(), task.getOrderId(), task.getAmount());

            try {
                ResponseEntity<PaymentApiResponse> response = restTemplate.postForEntity(REFUND_URL, apiRequest, PaymentApiResponse.class);
                PaymentApiResponse body = response.getBody();

                if (response.getStatusCode().is2xxSuccessful() && body != null && body.isSuccess()) {
                    // 退款成功，更新狀態並發布事件通知 Saga 補償結束
                    task.setStatus("SUCCESS");
                    task.setUpdateTime(LocalDateTime.now());
                    refundRetryTaskRepository.save(task);

                    log.info("[RefundRetryScheduler] 退款重試成功，發布 WalletRefundedEvent: orderId={}", task.getOrderId());
                    eventGateway.publish(new WalletRefundedEvent(task.getUserId(), task.getOrderId(), task.getAmount()));
                } else {
                    String errorMsg = body != null ? body.getMessage() : "原因未知";
                    handleFailedAttempt(task, errorMsg);
                }
            } catch (Exception e) {
                handleFailedAttempt(task, e.getMessage());
            }
        }
    }

    private void handleFailedAttempt(AxonRefundRetryTaskEntity task, String errorMessage) {
        int nextRetryCount = task.getRetryCount() + 1;
        task.setRetryCount(nextRetryCount);
        task.setLastErrorMessage(errorMessage != null ? (errorMessage.length() > 200 ? errorMessage.substring(0, 200) : errorMessage) : "退款異常");
        task.setUpdateTime(LocalDateTime.now());

        if (nextRetryCount >= MAX_RETRY_LIMIT) {
            // 超過最大重試次數，標記為失敗並發布最高級警報日誌以供運維工程師介入
            task.setStatus("FAILED");
            refundRetryTaskRepository.save(task);
            log.error("[ALERT] [REFUND SYSTEM] 嚴重警報：訂單 {} 的退款自動重試累計 {} 次全部失敗！任務已放棄，請工程師立即手動對帳排查！錯誤原因: {}",
                    task.getOrderId(), nextRetryCount, task.getLastErrorMessage());
        } else {
            // 指數退避算法計算下一次重試時間：30秒 * 2^retryCount (即第 1 次失敗後間隔 60秒，第 2 次間隔 120秒，依此類推)
            long backoffSeconds = 30L * (1L << nextRetryCount);
            task.setNextRetryTime(LocalDateTime.now().plusSeconds(backoffSeconds));
            refundRetryTaskRepository.save(task);
            log.warn("[RefundRetryScheduler] 退款重試失敗，將於 {} 秒後進行下一次嘗試: orderId={}, 錯誤={}",
                    backoffSeconds, task.getOrderId(), task.getLastErrorMessage());
        }
    }

    // --- 內部 HTTP DTO 類別 ---

    @Data
    public static class PaymentApiRequest {
        private String userId;
        private String orderId;
        private long amount;

        public PaymentApiRequest() {}
        public PaymentApiRequest(String userId, String orderId, long amount) {
            this.userId = userId;
            this.orderId = orderId;
            this.amount = amount;
        }
    }

    @Data
    public static class PaymentApiResponse {
        private boolean success;
        private String message;
    }
}
