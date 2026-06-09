package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.command.DebitWalletCommand;
import com.example.kafka_cqrs_demo.axon.command.RefundWalletCommand;
import com.example.kafka_cqrs_demo.axon.event.OrderCancelledEvent;
import com.example.kafka_cqrs_demo.axon.event.WalletDebitFailedEvent;
import com.example.kafka_cqrs_demo.axon.event.WalletDebitedEvent;
import com.example.kafka_cqrs_demo.axon.event.WalletRefundedEvent;
import com.example.kafka_cqrs_demo.axon.repository.AxonWalletTransactionRepository;
import com.example.kafka_cqrs_demo.entity.AxonWalletTransactionEntity;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

/**
 * 金流防腐層適配器 (Payment Adapter - Anti-Corruption Layer)
 * <p>
 * 本適配器將 Axon 的異步事件驅動 Command/Event 與外部金流系統的同步 HTTP 呼叫進行橋接。
 * 1. 接收 DebitWalletCommand 扣款指令，並發送 HTTP POST 請求至外部模擬扣款 API，再根據 API 響應發布成功/失敗領域事件。
 * 2. 接收 RefundWalletCommand 退款指令，並透過 HTTP 進行外部退款。
 * 3. 監聽 OrderCancelledEvent 訂單取消事件，對先前扣款成功的交易自動透過 HTTP 呼叫外部 API 進行退款補償。
 * </p>
 */
@Slf4j
@Service
public class PaymentAdapter {

    private final EventGateway eventGateway;
    private final AxonWalletTransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    private static final String BASE_URL = "http://localhost:8081/api/external/payment";

    public PaymentAdapter(EventGateway eventGateway,
                          AxonWalletTransactionRepository transactionRepository) {
        this.eventGateway = eventGateway;
        this.transactionRepository = transactionRepository;
        this.restTemplate = new RestTemplate(); // 使用簡單的內建實例，避免需要額外宣告 Bean
    }

    /**
     * 處理扣減錢包指令 (DebitWalletCommand)
     */
    @CommandHandler
    public void handle(DebitWalletCommand command) {
        log.info("[PaymentAdapter] 收到扣款指令，準備透過 HTTP 呼叫外部金流 API: userId={}, orderId={}, amount={}",
                command.getUserId(), command.getOrderId(), command.getAmount());

        String url = BASE_URL + "/debit";
        PaymentApiRequest apiRequest = new PaymentApiRequest(command.getUserId(), command.getOrderId(), command.getAmount());

        try {
            ResponseEntity<PaymentApiResponse> response = restTemplate.postForEntity(url, apiRequest, PaymentApiResponse.class);
            PaymentApiResponse body = response.getBody();

            if (response.getStatusCode().is2xxSuccessful() && body != null && body.isSuccess()) {
                log.info("[PaymentAdapter] 外部 HTTP 扣款成功，發布 WalletDebitedEvent: orderId={}", command.getOrderId());
                eventGateway.publish(new WalletDebitedEvent(command.getUserId(), command.getOrderId(), command.getAmount()));
            } else {
                String errorMsg = body != null ? body.getMessage() : "原因未知";
                log.warn("[PaymentAdapter] 外部 HTTP 扣款失敗: {}, 發布 WalletDebitFailedEvent", errorMsg);
                eventGateway.publish(new WalletDebitFailedEvent(command.getUserId(), command.getOrderId(), command.getAmount(), errorMsg));
            }
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            // 處理 HTTP 4xx / 5xx 錯誤
            String errorMsg = "HTTP錯誤";
            try {
                PaymentApiResponse errorBody = e.getResponseBodyAs(PaymentApiResponse.class);
                if (errorBody != null) {
                    errorMsg = errorBody.getMessage();
                }
            } catch (Exception ex) {
                errorMsg = e.getStatusText();
            }
            log.warn("[PaymentAdapter] 外部 HTTP 扣款異常 ({}): {}, 發布 WalletDebitFailedEvent", e.getStatusCode(), errorMsg);
            eventGateway.publish(new WalletDebitFailedEvent(command.getUserId(), command.getOrderId(), command.getAmount(), errorMsg));
        } catch (RestClientException e) {
            // 處理網絡連線異常
            log.error("[PaymentAdapter] 外部 HTTP 連線失敗: {}, 發布 WalletDebitFailedEvent", e.getMessage());
            eventGateway.publish(new WalletDebitFailedEvent(command.getUserId(), command.getOrderId(), command.getAmount(), "網絡連線異常: " + e.getMessage()));
        }
    }

    /**
     * 處理退款指令 (RefundWalletCommand)
     */
    @CommandHandler
    public void handle(RefundWalletCommand command) {
        log.info("[PaymentAdapter] 收到退款指令，準備透過 HTTP 呼叫外部金流 API: userId={}, orderId={}, amount={}",
                command.getUserId(), command.getOrderId(), command.getAmount());

        String url = BASE_URL + "/refund";
        PaymentApiRequest apiRequest = new PaymentApiRequest(command.getUserId(), command.getOrderId(), command.getAmount());

        try {
            ResponseEntity<PaymentApiResponse> response = restTemplate.postForEntity(url, apiRequest, PaymentApiResponse.class);
            PaymentApiResponse body = response.getBody();

            if (response.getStatusCode().is2xxSuccessful() && body != null && body.isSuccess()) {
                log.info("[PaymentAdapter] 外部 HTTP 退款成功，發布 WalletRefundedEvent: orderId={}", command.getOrderId());
                eventGateway.publish(new WalletRefundedEvent(command.getUserId(), command.getOrderId(), command.getAmount()));
            } else {
                log.warn("[PaymentAdapter] 外部 HTTP 退款失敗: {}", body != null ? body.getMessage() : "原因未知");
            }
        } catch (Exception e) {
            log.error("[PaymentAdapter] 外部 HTTP 退款例外: {}", e.getMessage(), e);
        }
    }

    /**
     * 監聽訂單取消事件，執行退款防腐補償
     */
    @EventHandler
    @Transactional
    public void on(OrderCancelledEvent event) {
        log.info("[PaymentAdapter] 收到訂單取消事件，準備檢查是否需要外部退款: orderId={}, reason={}",
                event.getOrderId(), event.getReason());

        // 查詢該訂單在外部金流中是否有成功扣款的交易流水 (本地資料庫作為外部對帳的鏡像)
        Optional<AxonWalletTransactionEntity> debitTxOpt = transactionRepository
                .findByOrderIdAndType(event.getOrderId(), "DEBIT");

        if (debitTxOpt.isPresent()) {
            AxonWalletTransactionEntity debitTx = debitTxOpt.get();
            if ("SUCCESS".equals(debitTx.getStatus())) {
                long refundAmount = -debitTx.getAmount(); // 扣款為負數，取相反數作為退款金額

                // 檢查是否已經退款過 (等冪性)
                Optional<AxonWalletTransactionEntity> refundTxOpt = transactionRepository
                        .findByOrderIdAndType(event.getOrderId(), "REFUND");

                if (refundTxOpt.isPresent()) {
                    log.info("[PaymentAdapter] 該訂單先前已成功退款，無需再次發起 HTTP 退款: orderId={}", event.getOrderId());
                    return;
                }

                log.info("[PaymentAdapter] 訂單 {} 先前已扣款成功，發送 HTTP 退款請求，金額: {} 元",
                        event.getOrderId(), refundAmount);

                String url = BASE_URL + "/refund";
                PaymentApiRequest apiRequest = new PaymentApiRequest(debitTx.getUserId(), event.getOrderId(), refundAmount);

                try {
                    ResponseEntity<PaymentApiResponse> response = restTemplate.postForEntity(url, apiRequest, PaymentApiResponse.class);
                    PaymentApiResponse body = response.getBody();

                    if (response.getStatusCode().is2xxSuccessful() && body != null && body.isSuccess()) {
                        log.info("[PaymentAdapter] 外部 HTTP 售後自動退款成功，發布 WalletRefundedEvent: orderId={}", event.getOrderId());
                        eventGateway.publish(new WalletRefundedEvent(debitTx.getUserId(), event.getOrderId(), refundAmount));
                    } else {
                        log.error("[PaymentAdapter] 外部 HTTP 售後自動退款失敗: {}", body != null ? body.getMessage() : "原因未知");
                    }
                } catch (Exception e) {
                    log.error("[PaymentAdapter] 外部 HTTP 售後自動退款例外: {}", e.getMessage(), e);
                }
            }
        } else {
            log.info("[PaymentAdapter] 該訂單未成功扣款，無需退款: orderId={}", event.getOrderId());
        }
    }

    // --- 內部 HTTP DTO 類別 ---

    @Data
    private static class PaymentApiRequest {
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
    private static class PaymentApiResponse {
        private boolean success;
        private String message;
    }
}
