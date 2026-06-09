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

### 2. Axon Saga 事件溯源模式 (Axon Module)
本模組實作了基於領域驅動設計 (DDD) 的事件溯源與 Saga 分散式事務協調器：
* **去 Axon Server 架構**：停用 Axon Server（`axon.axonserver.enabled: false`），顯式配置本地 `JpaEventStorageEngine`，並註冊自訂 `axonTransactionManager` 全域事務管理器，將事件日誌持久化於 MySQL 表格中。
* **持久化事件進度**：配置 `JpaTokenStore` 以便將事件處理器的進度進度（Token）儲存於資料庫（`token_entry`），避免應用程式重啟時重複重放所有歷史事件。
* **庫存預留與補償機制 (Saga)**：
  * 訂單建立後，由 `OrderSaga` 協調器非同步發送 `ReserveStockCommand` 至庫存模組。
  * 庫存充足時，扣減可用庫存並增加預留庫存，同時記錄 `t_axon_stock_reservation` 預留明細。
  * 若付款成功，正式扣除預留庫存，將預留記錄標記為 `COMPLETED`。
  * 若付款超時或手動取消，Saga 執行補償動作釋放預留量，並將可用庫存加回。
  * 支援已付款訂單售後取消（退貨），能自動將已扣除的庫存退回可用庫存。

---

## 資料庫實體與表格設計

Axon 模組使用以下 MySQL 表格進行狀態儲存：

1. **t_axon_order (訂單實體表)**
   * `order_id` (VARCHAR, 主鍵)
   * `product_id` (VARCHAR)
   * `quantity` (INT)
   * `price` (BIGINT)
   * `status` (VARCHAR, 例如: CREATED, PENDING_PAYMENT, PAID, CANCELLED)
   * `cancel_reason` (VARCHAR, 取消原因)
   * `create_time` (DATETIME)

2. **t_axon_inventory (庫存實體表)**
   * `product_id` (VARCHAR, 主鍵)
   * `stock` (INT, 當前可用庫存)
   * `reserved_stock` (INT, 被預留鎖定的庫存)

3. **t_axon_stock_reservation (庫存預留明細表)**
   * `order_id` (VARCHAR, 主鍵)
   * `product_id` (VARCHAR)
   * `quantity` (INT)
   * `status` (VARCHAR, 狀態: RESERVED, COMPLETED, RELEASED, REFUNDED)
   * `update_time` (DATETIME)

---

## 測試重置步驟 (Reset Environment)

在開始測試前，請依序清空舊資料以確保測試一致性。

### 1. 執行資料庫重置 SQL
在您的 MySQL 管理工具中對目標資料庫執行以下 SQL 語法：
```sql
-- 清空歷史業務資料
TRUNCATE TABLE t_axon_order;
TRUNCATE TABLE t_axon_stock_reservation;
TRUNCATE TABLE t_axon_inventory;

-- 清空 Axon 框架系統資料表
TRUNCATE TABLE domain_event_entry;
TRUNCATE TABLE snapshot_event_entry;
TRUNCATE TABLE association_value_entry;
TRUNCATE TABLE saga_entry;
TRUNCATE TABLE token_entry;
```

### 2. 清空 Redis 快取
在終端機執行：
```bash
redis-cli FLUSHALL
```

### 3. 啟動服務
重啟您的 Spring Boot 服務。啟動時，`AxonDatabaseInitializer` 會自動在 `t_axon_inventory` 表格中插入三筆預設測試商品數據：
* `PROD-001`：可用庫存 100 件，預留 0 件。
* `PROD-002`：可用庫存 5 件，預留 0 件。
* `PROD-003`：可用庫存 0 件，預留 0 件（供測試庫存扣減失敗與自動補償）。

---

## 手動測試順序與 SQL 驗證指南

服務啟動後（預設埠口為 8081），請使用以下四個場景進行功能驗證。

### 場景一：正常付款流程 (Happy Path)

驗證「下單 -> 扣減並預留庫存 -> 支付成功 -> 完成扣庫存」的正常流程。

#### 1. 發送建立訂單請求 (購買 PROD-001 數量 2 件)
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders' \
--header 'Content-Type: application/json' \
--data '{
    "productId": "PROD-001",
    "quantity": 2,
    "price": 100
}'
```
> **注意**：請記錄 API 回傳的訂單 UUID（以下用 `{orderId}` 代替）。

#### 2. 第一階段 SQL 驗證
```sql
-- 1. 驗證訂單狀態變更為 PENDING_PAYMENT
SELECT order_id, product_id, quantity, price, status, cancel_reason FROM t_axon_order WHERE order_id = '{orderId}';

-- 2. 驗證可用庫存已扣減 2，預留庫存增加 2 (PROD-001 應為 stock = 98, reserved_stock = 2)
SELECT product_id, stock, reserved_stock FROM t_axon_inventory WHERE product_id = 'PROD-001';

-- 3. 驗證生成一筆狀態為 RESERVED 且數量為 2 的預留明細
SELECT order_id, product_id, quantity, status FROM t_axon_stock_reservation WHERE order_id = '{orderId}';
```

#### 3. 對該訂單發送付款確認請求
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders/pay' \
--header 'Content-Type: application/json' \
--data '{
    "orderId": "{orderId}"
}'
```

#### 4. 第二階段 SQL 驗證
```sql
-- 1. 驗證訂單狀態成功變更為 PAID
SELECT order_id, status FROM t_axon_order WHERE order_id = '{orderId}';

-- 2. 驗證可用庫存仍為 98，但預留庫存清空為 0 (PROD-001 應為 stock = 98, reserved_stock = 0)
SELECT product_id, stock, reserved_stock FROM t_axon_inventory WHERE product_id = 'PROD-001';

-- 3. 驗證預留明細狀態更新為 COMPLETED
SELECT order_id, status FROM t_axon_stock_reservation WHERE order_id = '{orderId}';
```

---

### 場景二：未付款手動取消 (釋放預留庫存)

驗證在待付款狀態下，手動取消訂單後，預留庫存安全退回至可用庫存中。

#### 1. 發送建立訂單請求 (購買 PROD-001 數量 3 件)
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders' \
--header 'Content-Type: application/json' \
--data '{
    "productId": "PROD-001",
    "quantity": 3,
    "price": 100
}'
```
> **注意**：記錄 API 回傳的 `{orderId}`。此時 PROD-001 庫存狀態應為：`stock = 95, reserved_stock = 3`。

#### 2. 發送取消訂單請求
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders/cancel' \
--header 'Content-Type: application/json' \
--data '{
    "orderId": "{orderId}",
    "reason": "手動放棄購買"
}'
```

#### 3. SQL 驗證
```sql
-- 1. 驗證訂單狀態成功變更為 CANCELLED，且 cancel_reason 為 '手動放棄購買'
SELECT order_id, status, cancel_reason FROM t_axon_order WHERE order_id = '{orderId}';

-- 2. 驗證可用庫存與預留庫存已退回 (PROD-001 應回歸 stock = 98, reserved_stock = 0)
SELECT product_id, stock, reserved_stock FROM t_axon_inventory WHERE product_id = 'PROD-001';

-- 3. 驗證預留明細狀態更新為 RELEASED
SELECT order_id, status FROM t_axon_stock_reservation WHERE order_id = '{orderId}';
```

---

### 場景三：已付款訂單售後取消 (退貨退款)

驗證已完成付款的訂單進行取消時，可用庫存能夠正確回加。

#### 1. 建立訂單並完成付款 (購買 PROD-001 數量 5 件)
1. **建立訂單**：
   ```bash
   curl --location 'http://localhost:8081/axonsaga/api/orders' \
   --header 'Content-Type: application/json' \
   --data '{
       "productId": "PROD-001",
       "quantity": 5,
       "price": 100
   }'
   ```
   > **注意**：記錄 API 回傳的 `{orderId}`。
2. **對該訂單付款**：
   ```bash
   curl --location 'http://localhost:8081/axonsaga/api/orders/pay' \
   --header 'Content-Type: application/json' \
   --data '{
       "orderId": "{orderId}"
   }'
   ```
> **此時庫存狀態**：PROD-001 的 `stock = 93, reserved_stock = 0`。

#### 2. 對已付款的訂單發送取消（售後退款）請求
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders/cancel' \
--header 'Content-Type: application/json' \
--data '{
    "orderId": "{orderId}",
    "reason": "售後申請退款"
}'
```

#### 3. SQL 驗證
```sql
-- 1. 驗證訂單狀態變更為 CANCELLED，且 cancel_reason 為 '售後申請退款'
SELECT order_id, status, cancel_reason FROM t_axon_order WHERE order_id = '{orderId}';

-- 2. 驗證可用庫存已退回 (PROD-001 可用庫存應加回 5 件變為 stock = 98, reserved_stock = 0)
SELECT product_id, stock, reserved_stock FROM t_axon_inventory WHERE product_id = 'PROD-001';

-- 3. 驗證預留明細狀態更新為 REFUNDED
SELECT order_id, status FROM t_axon_stock_reservation WHERE order_id = '{orderId}';
```

---

### 場景四：庫存不足自動取消 (Saga 自動補償)

驗證商品庫存不足時，Saga 協調器捕獲失敗事件並自動發起補償指令將訂單狀態改為已取消。

#### 1. 發送建立訂單請求 (訂購無庫存商品 PROD-003 數量 1 件)
```bash
curl --location 'http://localhost:8081/axonsaga/api/orders' \
--header 'Content-Type: application/json' \
--data '{
    "productId": "PROD-003",
    "quantity": 1,
    "price": 100
}'
```
> **注意**：記錄 API 回傳的 `{orderId}`。

#### 2. SQL 驗證
```sql
-- 1. 驗證訂單狀態已自動更新為 CANCELLED，且 cancel_reason 為 '庫存不足'
SELECT order_id, status, cancel_reason FROM t_axon_order WHERE order_id = '{orderId}';

-- 2. 驗證庫存預留表格不會有該 orderId 的成功預留紀錄
SELECT order_id, status FROM t_axon_stock_reservation WHERE order_id = '{orderId}';
```