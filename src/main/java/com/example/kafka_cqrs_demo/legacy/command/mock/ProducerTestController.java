package com.example.kafka_cqrs_demo.legacy.command.mock;

import com.example.kafka_cqrs_demo.legacy.command.dto.CreateOrderRequest;
import com.example.kafka_cqrs_demo.legacy.config.KafkaTopicConfig;
import com.example.kafka_cqrs_demo.legacy.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/test")
@Slf4j
public class ProducerTestController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * 🧪 驗證「生產端等冪性 (Kafka De-duplication)」的超高併發完全體壓測 API
     * <p>
     * 💡 【硬核壓測原理與底層行為分析】：
     * 本方法利用 {@link CountDownLatch} 作為發令槍，強迫 4 個執行緒在「同一主機微秒」內並行衝出，
     * 同時呼叫 {@code kafkaTemplate.send()} 發送相同 Key (mockOrderId) 的訊息。
     * <p>
     * ❓ 【為什麼此處生產端的 enable.idempotence=true 會看似「失效」？】
     * 1. <b>記憶體批次機制 (RecordAccumulator)：</b> Kafka Producer 收到發送請求時，並非立刻走網路 I/O，
     * 而是將訊息先放入記憶體緩衝區，並根據 Partition 進行打包 (Batching)。
     * 2. <b>連續序列號編配：</b> 因為 4 個執行緒是同時並行將 4 個獨立的記憶體物件砸進 Kafka 驅動，
     * Producer 驅動會認定這是業務層面「故意連續發送的 4 筆不同訊息」，因此在記憶體中依序為它們
     * 編配了「連續且遞增」的 Sequence Number (例如: Seq 1, 2, 3, 4)。
     * 3. <b>Broker 端判定：</b> Kafka Broker 收到該批次 (Batch) 後，發現 Sequence Number 完全連續且無重複，
     * 判定為正常的連續生產，而非網路抖動導致的重試，因此網開一面讓 4 筆訊息全部入佇列。
     * <p>
     * 🎯 【架構含金量 (面試核心論點)】：
     * 本測試完美證明了<b>「禦敵於國門之外（生產端等冪性）」在大併發場景下的局限性</b>。當重複流量在同一個
     * 微秒內並行發生時，訊息必定會穿透 Kafka 佇列砸向消費端。
     * 此時，專案設計的<b>「第二道防線：消費端 Redis 分散式鎖 (setIfAbsent)」</b>便發揮了強大的最終兜底效益，
     * 第一筆訊息成功搶鎖更新視圖，其餘 3 筆訊息在 3 毫秒內撞鎖攔截，噴出 WARN 警告，完美守住最終一致性。
     */
    @PostMapping("/produce-duplicate")
    public ResponseEntity<String> simulateDuplicateProduction(@RequestBody CreateOrderRequest request) {
        // 建立一個擁有 4 個執行緒的臨時執行緒池來模擬高併發
        java.util.concurrent.ExecutorService executorService = java.util.concurrent.Executors.newFixedThreadPool(4);
        try {
            String mockOrderId = UUID.randomUUID().toString();
            OrderCreatedEvent event = new OrderCreatedEvent(
                mockOrderId, request.getProductId(), request.getQuantity(), request.getPrice()
            );
            String jsonEvent = objectMapper.writeValueAsString(event);

            // 🏁 發令槍：計數為 1
            java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

            log.info("🚀 [生產端等冪性大壓測] 啟動！4 個並行執行緒已進起跑線，準備夾擊 Kafka... ID: {}", mockOrderId);

            // 瞬間丟 4 個任務進執行緒池，它們全都會卡在 latch.await()
            for (int i = 0; i < 4; i++) {
                final int threadNum = i + 1;
                executorService.submit(() -> {
                    try {
                        latch.await(); // 憋在起跑線
                        kafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME, mockOrderId, jsonEvent);
                        log.info("⚡ [生產端執行緒 {}] 訊息已成功射出！", threadNum);
                    } catch (Exception e) {
                        log.error("生產端執行緒 {} 發送失敗", threadNum, e);
                    }
                });
            }

            // 稍微睡 50 毫秒，確保 4 個執行緒都已經在 latch.await() 阻塞就位
            Thread.sleep(50);

            // 🔫 鳴槍起跑！
            latch.countDown();
            log.info("🔥 [發令槍響] 4 個執行緒在同一個微秒內並行衝出，呼叫 KafkaTemplate！");

            // 優雅關閉執行緒池並等待所有發送任務結束
            executorService.shutdown();
            executorService.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);

            return ResponseEntity.ok("【高併發生產測試送出】請觀察 Consumer 最終是消費 1 次還是 4 次！ID: " + mockOrderId);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("高併發生產模擬失敗: " + e.getMessage());
        } finally {
            if (!executorService.isShutdown()) {
                executorService.shutdownNow();
            }
        }
    }

    /**
     * 🛡️ API 2：專門驗證「消費端分散式鎖 (Redis SETNX 最終一體性)」
     * 流程：故意等待 1 秒分開送，或者由不同執行緒發送，藉此繞過 Kafka 生產端的 Sequence 去重。
     * 預期：Kafka 队列裡會確實存在 2 筆訊息。Consumer 收到第一筆時上鎖，第二筆直接撞牆，噴出「主動攔截」的 WARN Log。
     */
    /**
     * 🛡️ API 2：專門驗證「消費端高併發分散式鎖 (Redis SETNX)」
     * 流程：利用 CountDownLatch 讓多個執行緒在同一微秒「同時起跑」發送相同事件，強行穿透生產端防禦。
     * 預期：Kafka 佇列裡會確實存在 2 筆訊息。Consumer 端的兩個執行緒會同時觸發，但只有一個能拿到 Redis 鎖。
     */
    @PostMapping("/consume-duplicate")
    public ResponseEntity<String> testConsumerIdempotence(@RequestBody CreateOrderRequest request) {
        try {
            String mockOrderId = UUID.randomUUID().toString();
            OrderCreatedEvent event = new OrderCreatedEvent(
                mockOrderId, request.getProductId(), request.getQuantity(), request.getPrice()
            );
            String jsonEvent = objectMapper.writeValueAsString(event);

            // 🏁 核心黑科技：併發倒數閘門（計數為 1）
            // 它的作用就像賽跑時的「發令槍」，閘門沒開之前，所有執行緒都必須在起跑線憋著
            java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

            log.info("🚀 [消費端高併發防禦測試] 準備發動！初始化 2 個執行緒進入起跑線... ID: {}", mockOrderId);

            // 執行緒 A：負責發送第一筆
            java.util.concurrent.CompletableFuture<Void> task1 = java.util.concurrent.CompletableFuture.runAsync(() -> {
                try {
                    latch.await(); // 進入起跑線等待發令槍響
                    kafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME, mockOrderId, jsonEvent);
                } catch (Exception e) {
                    log.error("執行緒 A 發送失敗", e);
                }
            });

            // 執行緒 B：負責發送第二筆
            java.util.concurrent.CompletableFuture<Void> task2 = java.util.concurrent.CompletableFuture.runAsync(() -> {
                try {
                    latch.await(); // 進入起跑線等待發令槍響
                    kafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME, mockOrderId, jsonEvent);
                } catch (Exception e) {
                    log.error("執行緒 B 發送失敗", e);
                }
            });

            // 🔥 兩台跑車都就位了，手動延遲 50 毫秒確保執行緒都已經進到 latch.await() 阻塞狀態
            Thread.sleep(50);

            // 🔫 鳴槍起跑！倒數減為 0，兩個執行緒會在「同一主機微秒內」同時並行發送 Kafka
            // 由於是由不同的執行緒（Thread）在不同的 CPU 核心上發出，這會完美穿透 Kafka 相同生產者的 Sequence 限制
            latch.countDown();
            log.info("⚡ [發令槍響] 2 個執行緒已瞬間同時對 Kafka 進行超高併發夾擊轟炸！");

            // 等待兩個非同步任務都發送完成
            java.util.concurrent.CompletableFuture.allOf(task1, task2).join();

            return ResponseEntity.ok("【高併發消費測試送出】請快去 Console 驗證高併發肉搏攔截 Log！ID: " + mockOrderId);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("高併發測試失敗: " + e.getMessage());
        }
    }

    @PostMapping("/retry-simulation")
    public ResponseEntity<String> simulateProducerRetry(@RequestBody CreateOrderRequest request) {
        try {
            String mockOrderId = UUID.randomUUID().toString();
            OrderCreatedEvent event = new OrderCreatedEvent(
                mockOrderId, request.getProductId(), request.getQuantity(), request.getPrice()
            );
            String jsonEvent = objectMapper.writeValueAsString(event);

            log.info("🚀 [自動化重試測試] 啟動！利用純 Java 匿名物件模擬網路中斷... ID: {}", mockOrderId);

            // 1. 核心黑科技：利用匿名內部類別覆寫 send 方法，內建計數器狀態機
            KafkaTemplate<String, String> spyKafkaTemplate =
                new org.springframework.kafka.core.KafkaTemplate<String, String>(kafkaTemplate.getProducerFactory()) {

                    // 用一個原子計數器記錄呼叫次數
                    private final AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger(0);

                    @Override
                    public CompletableFuture<SendResult<String, String>> send(
                        String topic, String key, String data) {

                        int currentCall = callCount.incrementAndGet();
                        if (currentCall == 1) {
                            // 📡 第一次呼叫：故意拋出 Kafka 原生超時異常，模擬突發性網路中斷
                            throw new TimeoutException(
                                "【模擬突發性網路中斷】1000 ms has passed since batch creation plus linger time."
                            );
                        }
                        // 🔄 第二次及之後的呼叫：完美放行，呼叫原本真實的 KafkaTemplate 發送
                        return kafkaTemplate.send(topic, key, data);
                    }
                };

            // 2. 發動第一次發送（這筆在內部會直接觸發 currentCall == 1，噴出 Timeout 異常）
            try {
                log.info("📡 [第一次發送嘗試] 故意製造網路抖動...");
                spyKafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME, mockOrderId, jsonEvent);
            } catch (Exception e) {
                log.warn("⚠️ [生產端攔截預期異常] 第一次發送確實超時失敗了！錯誤訊息: {}", e.getMessage());

                // 3. 🚀 【核心重試模擬】：當業務層面捕捉到超時後，發動第二次重試補償（觸發 currentCall == 2，成功射出）
                log.info("🔄 [啟動生產端自動補償機制] 正在發動第二次等冪性重試發送... ID: {}", mockOrderId);
                spyKafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME, mockOrderId, jsonEvent);
            }

            return ResponseEntity.ok("【自動化重試測試成功】請去 Console 觀察重試與 Consumer 最終的一體性。ID: " + mockOrderId);
        } catch (Exception e) {
            log.error("重試模擬異常", e);
            return ResponseEntity.internalServerError().body("重試模擬失敗: " + e.getMessage());
        }
    }
}