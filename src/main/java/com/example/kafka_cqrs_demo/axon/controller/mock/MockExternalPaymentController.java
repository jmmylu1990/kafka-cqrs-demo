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
import java.util.Optional;
import java.util.UUID;

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

    public MockExternalPaymentController(AxonWalletRepository walletRepository,
                                         AxonWalletTransactionRepository transactionRepository) {
        this.walletRepository = walletRepository;
        this.transactionRepository = transactionRepository;
    }

    /**
     * 模擬外部扣款 API
     */
    @PostMapping("/debit")
    public ResponseEntity<PaymentResponse> debit(@RequestBody PaymentRequest request) {
        log.info("[MockExternalAPI] 收到 HTTP 扣款請求: userId={}, orderId={}, amount={}",
                request.getUserId(), request.getOrderId(), request.getAmount());

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
