package com.example.kafka_cqrs_demo.axon.controller.mock;

import com.example.kafka_cqrs_demo.axon.repository.AxonWalletRepository;
import com.example.kafka_cqrs_demo.axon.repository.AxonWalletTransactionRepository;
import com.example.kafka_cqrs_demo.entity.AxonWalletEntity;
import com.example.kafka_cqrs_demo.entity.AxonWalletTransactionEntity;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 模擬外部金流平台 REST 控制器 (Mock External Payment Controller)
 * <p>
 * 本類別模擬一個獨立的外部第三方支付平台（例如 Stripe、Line Pay 服務端）。
 * 它暴露扣款與退款的 HTTP API，並接管本地資料表 t_axon_wallet 與 t_axon_wallet_transaction 的更新，
 * 藉此模擬外部支付系統內部的數據變更與流水記帳。
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("/api/external/payment")
public class MockExternalPaymentController {

    private final AxonWalletRepository walletRepository;
    private final AxonWalletTransactionRepository transactionRepository;

    // 故障模擬控制變數
    private final AtomicBoolean debitTimeoutEnabled = new AtomicBoolean(false);
    private final AtomicBoolean refundFailureEnabled = new AtomicBoolean(false);

    public MockExternalPaymentController(AxonWalletRepository walletRepository,
                                         AxonWalletTransactionRepository transactionRepository) {
        this.walletRepository = walletRepository;
        this.transactionRepository = transactionRepository;
    }

    /**
     * 啟用或關閉扣款超時模擬
     */
    @PutMapping("/control/debit-timeout")
    public ResponseEntity<String> controlDebitTimeout(@RequestParam boolean enable) {
        debitTimeoutEnabled.set(enable);
        log.info("[MockExternalAPI] 控制端點設定扣款超時模擬為: {}", enable);
        return ResponseEntity.ok("設定成功，debit-timeout: " + enable);
     }

     /**
      * 啟用或關閉退款隨機失敗模擬
      */
     @PutMapping("/control/refund-fail")
     public ResponseEntity<String> controlRefundFail(@RequestParam boolean enable) {
         refundFailureEnabled.set(enable);
         log.info("[MockExternalAPI] 控制端點設定退款失敗模擬為: {}", enable);
         return ResponseEntity.ok("設定成功，refund-fail: " + enable);
     }

    /**
     * 查詢當前故障模擬開關狀態
     */
    @GetMapping("/control/status")
    public ResponseEntity<Map<String, Boolean>> getControlStatus() {
        Map<String, Boolean> status = new HashMap<>();
        status.put("debitTimeoutEnabled", debitTimeoutEnabled.get());
        status.put("refundFailureEnabled", refundFailureEnabled.get());
        return ResponseEntity.ok(status);
    }

    /**
     * 模擬外部扣款 API
     */
    @PostMapping("/debit")
    public ResponseEntity<PaymentResponse> debit(@RequestBody PaymentRequest request) {
        log.info("[MockExternalAPI] 收到 HTTP 扣款請求: userId={}, orderId={}, amount={}",
                request.getUserId(), request.getOrderId(), request.getAmount());

        // 模擬超時：延遲 5 秒以觸發呼叫端 RestTemplate 的 3 秒 Timeout 機制
        if (debitTimeoutEnabled.get()) {
            log.warn("[MockExternalAPI] 偵測到扣款超時模擬開啟，準備延遲 5 秒以觸發呼叫端 Timeout...");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 1. 等冪性檢查
        Optional<AxonWalletTransactionEntity> existTx = transactionRepository
                .findByOrderIdAndType(request.getOrderId(), "DEBIT");
        if (existTx.isPresent()) {
            AxonWalletTransactionEntity tx = existTx.get();
            if ("SUCCESS".equals(tx.getStatus())) {
                log.info("[MockExternalAPI] 該訂單先前已扣款成功，回傳成功響應: orderId={}", request.getOrderId());
                return ResponseEntity.ok(new PaymentResponse(true, "該訂單已成功扣款(等冪)"));
            }
        }

        // 2. 錢包是否存在
        Optional<AxonWalletEntity> walletOpt = walletRepository.findById(request.getUserId());
        if (walletOpt.isEmpty()) {
            log.warn("[MockExternalAPI] 扣款失敗：使用者 {} 的錢包不存在", request.getUserId());
            recordTransaction(request.getUserId(), request.getOrderId(), request.getAmount(), "DEBIT", "FAILED");
            return ResponseEntity.badRequest().body(new PaymentResponse(false, "使用者錢包不存在"));
        }

        // 3. 校驗餘額
        AxonWalletEntity wallet = walletOpt.get();
        if (wallet.getBalance() < request.getAmount()) {
            log.warn("[MockExternalAPI] 扣款失敗：使用者 {} 餘額不足。剩餘餘額: {}, 欲扣款: {}",
                    request.getUserId(), wallet.getBalance(), request.getAmount());
            recordTransaction(request.getUserId(), request.getOrderId(), request.getAmount(), "DEBIT", "FAILED");
            return ResponseEntity.badRequest().body(new PaymentResponse(false, "餘額不足"));
        }

        // 4. 執行扣款
        wallet.setBalance(wallet.getBalance() - request.getAmount());
        walletRepository.save(wallet);

        // 5. 記錄交易明細 (扣款存為負數)
        recordTransaction(request.getUserId(), request.getOrderId(), -request.getAmount(), "DEBIT", "SUCCESS");

        log.info("[MockExternalAPI] 扣款成功！使用者 {} 餘額扣除 {} 元，目前餘額: {} 元",
                request.getUserId(), request.getAmount(), wallet.getBalance());

        return ResponseEntity.ok(new PaymentResponse(true, "扣款成功"));
    }

    /**
     * 模擬外部退款 API
     */
    @PostMapping("/refund")
    public ResponseEntity<PaymentResponse> refund(@RequestBody PaymentRequest request) {
        log.info("[MockExternalAPI] 收到 HTTP 退款請求: userId={}, orderId={}, amount={}",
                request.getUserId(), request.getOrderId(), request.getAmount());

        // 模擬退款故障：直接回傳 HTTP 500 狀態碼
        if (refundFailureEnabled.get()) {
            log.warn("[MockExternalAPI] 偵測到退款失敗模擬開啟，直接返回 HTTP 500 錯誤...");
            return ResponseEntity.status(500).body(new PaymentResponse(false, "模擬外部退款異常(伺服器錯誤)"));
        }

        // 1. 等冪性檢查
        Optional<AxonWalletTransactionEntity> existTx = transactionRepository
                .findByOrderIdAndType(request.getOrderId(), "REFUND");
        if (existTx.isPresent()) {
            log.info("[MockExternalAPI] 該訂單先前已退款完成，回傳成功響應: orderId={}", request.getOrderId());
            return ResponseEntity.ok(new PaymentResponse(true, "該訂單已退款完成(等冪)"));
        }

        // 2. 獲取錢包
        Optional<AxonWalletEntity> walletOpt = walletRepository.findById(request.getUserId());
        if (walletOpt.isEmpty()) {
            log.error("[MockExternalAPI] 退款失敗：使用者 {} 的錢包不存在", request.getUserId());
            return ResponseEntity.badRequest().body(new PaymentResponse(false, "使用者錢包不存在"));
        }

        // 3. 退回金額
        AxonWalletEntity wallet = walletOpt.get();
        wallet.setBalance(wallet.getBalance() + request.getAmount());
        walletRepository.save(wallet);

        // 4. 記錄退款明細 (退款存為正數)
        recordTransaction(request.getUserId(), request.getOrderId(), request.getAmount(), "REFUND", "SUCCESS");

        log.info("[MockExternalAPI] 退款成功！使用者 {} 餘額加回 {} 元，目前餘額: {} 元",
                request.getUserId(), request.getAmount(), wallet.getBalance());

        return ResponseEntity.ok(new PaymentResponse(true, "退款成功"));
    }

    private void recordTransaction(String userId, String orderId, long amount, String type, String status) {
        AxonWalletTransactionEntity transaction = new AxonWalletTransactionEntity(
                UUID.randomUUID().toString(),
                userId,
                orderId,
                amount,
                type,
                status,
                LocalDateTime.now()
        );
        transactionRepository.save(transaction);
    }

    // --- DTO 內部類別 ---

    @Data
    public static class PaymentRequest {
        private String userId;
        private String orderId;
        private long amount;
    }

    @Data
    public static class PaymentResponse {
        private boolean success;
        private String message;

        public PaymentResponse() {}
        public PaymentResponse(boolean success, String message) {
            this.success = success;
            this.message = message;
        }
    }
}
