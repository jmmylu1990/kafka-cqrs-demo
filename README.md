# High-Concurrency Event-Driven CQRS Order System Demo

![Java](https://img.shields.io/badge/Java-17%20--%2020-orange.svg)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x%20%2F%204.x-brightgreen.svg)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.1.2-black.svg)
![Redis](https://img.shields.io/badge/Redis-7.x-red.svg)
![MySQL](https://img.shields.io/badge/MySQL-8.0-blue.svg)

本專案是一個基於 **Spring Boot 3.x/4.x** 與 **Java 17/20** 構建的高性能、具備超高併發潛力的**事件驅動型 CQRS（命令查詢職責分離）** 訂單系統核心展示。

專案專注於解決分散式微服務架構中的**讀寫吞吐量不對稱**、**大流量下資料庫連線池保護**、以及**非同步最終一致性空窗期**等實務痛點。程式碼秉持 Clean Code 與防禦性程式設計原則，內建極致的技術壓測驗證模組，達到業界資深後端架構師的實作標準。

---

## 🚀 架架構核心亮點與設計細節

### 1. 職責分離與完全解耦 (CQRS & Event-Driven)
* **Command 端 (寫入陣營)**：專注狀態變更。接收 `POST` 請求並進行嚴格的防禦性參數校驗 (`@Valid`)，將訂單持久化至 MySQL（初始化為 `PENDING` 狀態），隨即非同步外發 Kafka 事件。
* **Query 端 (讀取陣營)**：高性能唯讀架構。透過背景 Consumer 監聽 Kafka 事件，即時更新讀取端專用的快取資料庫（長效唯讀視圖），面對海量 `GET` 請求做到不查資料庫、毫秒級秒殺回傳。

### 2. 核心性能優化：交易事務外置模式
* **痛點**：傳統做法會用全域 `@Transactional` 包裹資料庫操作與外部 Kafka/Redis 的網路 I/O。在高併發下，外部網路延遲會拉長事務時間，導致 MySQL 連線池瞬間被吃滿而崩潰。
* **解法**：專案採用**事務外置設計**，將實體資料庫寫入侷限在細粒度的隔離方法中。利用 Spring 原生 `@Lazy` 註解（或職責分離 Executor）優雅破解新版 Spring 預設禁用的循環依賴（Circular Reference），確保 **「MySQL 交易提交、連線釋放歸還後，才發動非同步網路 I/O」**。

### 3. 百萬級併發雙重等冪性防線 (Dual-Layer Idempotency)
為應對分散式墨菲定律（網路阻斷、服務重啟、水平擴充），本專案建立了前後雙重鐵壁防線：
* **前線：生產端等冪性發送**：
  在 `application.yml` 中完美配置 `ack: all`、`retries: 3`、以及用雙引號嚴格對齊限制的 `"max.in.flight.requests.per.connection": 5`，啟動 Kafka 內建的 **PID + Sequence Number** 機制，在 Broker 端全自動去重，防禦微觀層面的網路重試。
* **後方：消費端分散式鎖**：
  在背景 Consumer 引入 **Redis `setIfAbsent` (SETNX) 分散式鎖** 進行業務兜底。10 分鐘內相同 `orderId` 無法重複消費，徹底封死因 Rebalance、Pod 重啟等引發的重複消費問題。

### 4. 局部順序性保證 (Kafka Ordering)
* 發送事件時，精準傳入 `orderId` (UUID) 作為 **Kafka Key**。
* 透過 Kafka 內建的 Partitioner 進行 Hash 路由，**確保同一筆訂單的所有狀態變更事件皆進入同一個 Partition**，在分散式多執行緒併發下完美保證訊息的「消費順序性」。

### 5. 雙快取防禦策略：封死最終一致性空窗期
* **痛點**：Kafka 非同步同步存在微秒級延遲。若前端在下單後 0.001 秒內立刻發送 `GET` 輪詢，會因為唯讀視圖尚未建立而查到空資料。