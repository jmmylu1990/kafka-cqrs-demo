# High-Concurrency Event-Driven CQRS Order System Demo

本專案是一個基於 Spring Boot 3.x/4.x 與 Java 17/20 構建的高性能、具備超高併發潛力的事件驅動型 CQRS（命令查詢職責分離）與 Axon Saga 訂單系統核心展示。

專案專注於解決分散式微服務架構中的讀寫吞吐量不對稱、大流量下資料庫連線池保護、以及非同步最終一致性空窗期等實務痛點。專案程式碼分為傳統 Kafka/Redis 自研 CQRS 模式（Legacy 模組）與基於 Axon Framework + Saga 協調器 + MySQL 持久化的事件溯源模式（Axon 模組）。本專案已完全停用外部 Axon Server，改由本地關係型資料庫進行事件儲存與交易協調。

---

## 架構核心亮點與設計細節

### 1. 傳統自研 CQRS 模式 (Legacy Module)
* **Command 端 (寫入端)**：專注狀態變更。接收寫入請求並進行嚴格的防禦性參數校驗，將訂單持久化至 MySQL，隨即非同步發送 Kafka 事件，保障低延遲寫入。
* **Query 端 (讀取端)**：高性能唯讀架構。透過背景 Consumer 監聽 Kafka 事件，即時更新讀取端專用的快取資料庫（Redis 唯讀視圖），面對海量 GET 請求做到不查主要資料庫、毫秒級回傳。
* **交易事務外置模式**：將實體資料庫寫入侷限在細粒度的隔離方法中，避免用全域事務包裹外部 Kafka/Redis 網路 I/O。利用 Spring 原生 Lazy 註解優雅破解循環依賴，確保「MySQL 交易提交、連線釋放歸還後，才發動非同步網路 I/O」。
* **等冪性與順序性**：
  * 生產端配置 `acks=all`、`retries=3`、`max.in.flight.requests.per.connection=5`，開啟 Kafka 內建等冪性發送。
  * 消費端引入 Redis 分散式鎖進行業務去重，防止重複消費。
  * 發送事件時以 `orderId` 作為 Kafka Key，確保同一筆訂單的事件進入同一個 Partition，保證消費順序性。

### 2. Axon Saga 事件溯源與高併發設計 (Axon Module)
本模組實作了基於領域驅動設計 (DDD) 的事件溯源、Saga 分散式事務協調與高併發寫入優化：
* **去 Axon Server 架構**：停用 Axon Server（`axon.axonserver.enabled: false`），改由本地 MySQL 與 Redis 處理狀態。
* **Redisson 分散式鎖庫存扣減**：
  * 收到 `ReserveStockCommand` 時，透過 Redisson Client 的商品分散式鎖 (`RLock`) 鎖定商品 ID。
  * 在臨界區內執行 Redis `RBucket` 與 `RMap` 的原子庫存檢查、扣減與記錄，徹底防範高併發下的超賣問題，並利用 Watchdog 機制自動續期，防止鎖提前失效。
* **Kafka 最終一致性同步**：Redis 扣減或釋放成功後，發送 `InventorySyncEvent` 訊息至 Kafka `inventory-sync-events` 主題，由背景 Consumer 異步落庫寫回 MySQL，保證最終一致性。
* **模擬外部金流防腐層 (PaymentAdapter/ACL)**：
  * 移除本地資料庫強耦合，解耦為獨立外部金流 API (`MockExternalPaymentController`)。
  * `PaymentAdapter` 扮演防腐層，透過 HTTP REST API 請求外部扣款/退款，將結果轉譯為 Axon 事件推動 Saga。

---

## 資料庫實體與表格設計

Axon 模組使用以下 MySQL 表格進行狀態儲存：

1. **t_axon_order (訂單實體表)**
   * `order_id` (VARCHAR, 主鍵)
   * `product_id` (VARCHAR)
   * `quantity` (INT)
   * `price` (BIGINT)
   * `status` (VARCHAR, 例如: CREATED, PENDING_PAYMENT, PAID, CANCELLED)
   * `cancel_reason` (VARCHAR)
   * `create_time` (DATETIME)
   * `user_id` (VARCHAR)

2. **t_axon_inventory (庫存實體表)**
   * `product_id` (VARCHAR, 主鍵)
   * `stock` (INT, 當前可用庫存)
   * `reserved_stock` (INT, 預留鎖定的庫存)

3. **t_axon_stock_reservation (庫存預留明細表)**
   * `order_id` (VARCHAR, 主鍵)
   * `product_id` (VARCHAR)
   * `quantity` (INT)
   * `status` (VARCHAR, 狀態: RESERVED, COMPLETED, RELEASED, REFUNDED)
   * `update_time` (DATETIME)

4. **t_axon_wallet (錢包帳戶表 - 模擬外部金流)**
   * `user_id` (VARCHAR, 主鍵)
   * `balance` (BIGINT, 餘額)

5. **t_axon_wallet_transaction (錢包交易明細表 - 模擬外部金流)**
   * `transaction_id` (VARCHAR, 主鍵)
   * `user_id` (VARCHAR)
   * `order_id` (VARCHAR)
   * `amount` (BIGINT)
   * `type` (VARCHAR, DEBIT/REFUND)
   * `status` (VARCHAR, SUCCESS/FAILED)
   * `create_time` (DATETIME)

---

## 測試重置步驟 (Reset Environment)

在開始測試前，請依序清空舊資料以確保測試一致性。

### 1. 執行資料庫重置 SQL
在您的 MySQL 管理工具中對目標資料庫執行以下 SQL 語法：
```sql
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE t_axon_order;
TRUNCATE TABLE t_axon_stock_reservation;
TRUNCATE TABLE t_axon_inventory;
TRUNCATE TABLE t_axon_wallet;
TRUNCATE TABLE t_axon_wallet_transaction;
TRUNCATE TABLE domain_event_entry;
TRUNCATE TABLE token_entry;
TRUNCATE TABLE saga_entry;
TRUNCATE TABLE association_value_entry;
SET FOREIGN_KEY_CHECKS = 1;
```

### 2. 清空 Redis 快取
在終端機執行：
```bash
redis-cli FLUSHALL
```

### 3. 啟動服務
重啟您的 Spring Boot 服務。啟動時，`AxonDatabaseInitializer` 會自動在資料庫與 Redis 中配置測試資料：
* 商品 `PROD-001`：庫存 100 件（Redis Key: `product:PROD-001:stock`）。
* 使用者 `USER-001`：餘額 1000 元。
* 使用者 `USER-002`：餘額 10 元。

---

## 手動測試流程與 SQL 驗證指南

本專案啟動後（預設埠口為 8081），請使用 Postman/cURL 依序執行以下場景：

### 場景一：正常付款流程 (Happy Path)
* 使用者 `USER-001` 購買 **5 件** 單價 **10 元** 的商品（總金額 50 元）。

#### 1. 發送建立訂單請求
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders' \
--header 'Content-Type: application/json' \
--data '{
    "productId": "PROD-001",
    "quantity": 5,
    "price": 10,
    "userId": "USER-001"
}'
```
> **注意**：請複製 API 回傳的 `{orderId}`。

#### 2. 第一階段：檢查庫存已於 Redis 預留，並非同步落庫 MySQL
* **Redis 快取檢查**：
  ```bash
  # 可用庫存扣減為 95
  redis-cli GET product:PROD-001:stock
  # 預留庫存增加為 5
  redis-cli GET product:PROD-001:reserved
  # 訂單預留哈希表狀態為 RESERVED
  redis-cli HGETALL order:{orderId}:reservation
  ```
* **MySQL SQL 驗證**：
  ```sql
  -- 可用/預留庫存同步為 95 / 5
  SELECT product_id, stock, reserved_stock FROM t_axon_inventory WHERE product_id = 'PROD-001';
  -- 預留明細記錄狀態為 RESERVED
  SELECT order_id, product_id, quantity, status FROM t_axon_stock_reservation WHERE order_id = '{orderId}';
  -- 訂單狀態為 PENDING_PAYMENT
  SELECT order_id, status, user_id FROM t_axon_order WHERE order_id = '{orderId}';
  ```

#### 3. 對該訂單發送付款確認請求 (無須帶入 userId)
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders/pay' \
--header 'Content-Type: application/json' \
--data '{
    "orderId": "{orderId}"
}'
```

#### 4. 第二階段：檢查付款完成，預留庫存扣減，金額被扣除
* **Redis 快取檢查**：
  ```bash
  # 預留庫存歸零為 0
  redis-cli GET product:PROD-001:reserved
  # 訂單預留雜湊狀態更新為 COMPLETED
  redis-cli HGET order:{orderId}:reservation status
  ```
* **MySQL SQL 驗證**：
  ```sql
  -- 可用 95, 預留 0
  SELECT product_id, stock, reserved_stock FROM t_axon_inventory WHERE product_id = 'PROD-001';
  -- 預留明細更新為 COMPLETED
  SELECT status FROM t_axon_stock_reservation WHERE order_id = '{orderId}';
  -- 訂單狀態更新為 PAID
  SELECT order_id, status FROM t_axon_order WHERE order_id = '{orderId}';
  -- USER-001 錢包餘額由 1000 扣除為 950
  SELECT user_id, balance FROM t_axon_wallet WHERE user_id = 'USER-001';
  -- 交易流水為 SUCCESS (-50)
  SELECT user_id, amount, type, status FROM t_axon_wallet_transaction WHERE order_id = '{orderId}';
  ```

---

### 場景二：餘額不足導致扣款失敗 (Saga 自動回滾釋放 Redis 庫存)
* 使用者 `USER-002`（餘額僅 10 元）購買 **5 件** 單價 **10 元** 的商品（總金額 50 元）。

#### 1. 發送建立訂單請求
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders' \
--header 'Content-Type: application/json' \
--data '{
    "productId": "PROD-001",
    "quantity": 5,
    "price": 10,
    "userId": "USER-002"
}'
```
> **注意**：記錄 API 回傳的第二個 `{orderId}`。

#### 2. 送出付款確認請求
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders/pay' \
--header 'Content-Type: application/json' \
--data '{
    "orderId": "{orderId}"
}'
```

#### 3. 第三階段：檢查扣款失敗，庫存安全退回
* **Redis 快取檢查**：
  ```bash
  # 可用庫存安全加回，維持 95
  redis-cli GET product:PROD-001:stock
  # 預留庫存歸零為 0
  redis-cli GET product:PROD-001:reserved
  # 狀態變更為 RELEASED
  redis-cli HGET order:{orderId}:reservation status
  ```
* **MySQL SQL 驗證**：
  ```sql
  -- MySQL 可用 95, 預留 0
  SELECT product_id, stock, reserved_stock FROM t_axon_inventory WHERE product_id = 'PROD-001';
  -- 預留明細更新為 RELEASED
  SELECT status FROM t_axon_stock_reservation WHERE order_id = '{orderId}';
  -- 訂單狀態更新為 CANCELLED，原因為扣款失敗
  SELECT order_id, status, cancel_reason FROM t_axon_order WHERE order_id = '{orderId}';
  -- USER-002 餘額仍為 10 元
  SELECT user_id, balance FROM t_axon_wallet WHERE user_id = 'USER-002';
  ```

---

### 場景三：已付款訂單售後取消 (自動退款補償)
* 針對場景一成功付款的訂單進行取消。

#### 1. 發送取消請求
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders/cancel' \
--header 'Content-Type: application/json' \
--data '{
    "orderId": "{場景一成功的orderId}",
    "reason": "售後七天無條件退貨"
}'
```

#### 2. 第四階段：檢查錢包加回，Redis 可用庫存退回
* **Redis 快取檢查**：
  ```bash
  # 可用庫存退回，由 95 增加為 100
  redis-cli GET product:PROD-001:stock
  # 狀態更新為 REFUNDED
  redis-cli HGET order:{場景一成功的orderId}:reservation status
  ```
* **MySQL SQL 驗證**：
  ```sql
  -- 可用庫存回復至 100, 預留 0
  SELECT product_id, stock, reserved_stock FROM t_axon_inventory WHERE product_id = 'PROD-001';
  -- 預留記錄狀態更新為 REFUNDED
  SELECT status FROM t_axon_stock_reservation WHERE order_id = '{場景一成功的orderId}';
  -- 訂單狀態更新為 CANCELLED，取消原因為退貨
  SELECT order_id, status, cancel_reason FROM t_axon_order WHERE order_id = '{場景一成功的orderId}';
  -- USER-001 餘額退回至 1000 元
  SELECT user_id, balance FROM t_axon_wallet WHERE user_id = 'USER-001';
  ```

---

### 場景四：商品庫存不足自動取消 (Saga 自動補償)
* 針對初始庫存為 0 的商品 `PROD-003` 進行下單，驗證 Saga 自動補償回滾。

#### 1. 發送建立訂單請求 (購買 PROD-003 數量 1 件)
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders' \
--header 'Content-Type: application/json' \
--data '{
    "productId": "PROD-003",
    "quantity": 1,
    "price": 100,
    "userId": "USER-001"
}'
```
> **注意**：請複製此時 API 回傳的第三個 `{orderId}`。

#### 2. 第五階段：檢查庫存預留失敗，訂單自動回滾取消
* **Redis 快取檢查**：
  ```bash
  # 可用庫存依然維持為 0
  redis-cli GET product:PROD-003:stock
  # 預留庫存也維持為 0
  redis-cli GET product:PROD-003:reserved
  # 不會有此訂單的 reservation 雜湊表記錄
  redis-cli KEYS order:{orderId}:reservation
  ```
* **MySQL SQL 驗證**：
  ```sql
  -- 1. 驗證訂單狀態已自動更新為 CANCELLED，且原因為 '庫存不足'
  SELECT order_id, status, cancel_reason FROM t_axon_order WHERE order_id = '{orderId}';

  -- 2. 驗證庫存預留表格不會有該 orderId 的成功預留紀錄
  SELECT order_id, status FROM t_axon_stock_reservation WHERE order_id = '{orderId}';
  ```
```