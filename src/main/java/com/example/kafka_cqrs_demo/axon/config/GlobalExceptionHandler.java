package com.example.kafka_cqrs_demo.axon.config;

import org.axonframework.commandhandling.CommandExecutionException;
import org.axonframework.modelling.command.AggregateNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

/**
 * 全域異常處理器 (Global Exception Handler)
 * <p>
 * 本類別採用 Spring MVC 的 RestControllerAdvice 機制，用以攔截控制器 (Controller) 層拋出的所有未處理異常。
 * 特別針對 Axon Framework 丟出的領域模型異常（例如找不到聚合根、違反狀態機規則等）進行統一格式包裝，
 * 以便向前端與 API 呼叫端返回友善且語意清晰的 JSON 錯誤回應。
 * </p>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 處理找不到訂單聚合根的異常 (AggregateNotFoundException)。
     * 當透過查詢或指令查找不存在的訂單 ID 時，Axon 會拋出此異常。
     * 本方法將其轉化為 HTTP 404 Not Found 回應。
     *
     * @param ex 找不到聚合根異常
     * @return 包含自訂錯誤結構的 ResponseEntity
     */
    @ExceptionHandler(AggregateNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleAggregateNotFound(AggregateNotFoundException ex) {
        Map<String, Object> body = new HashMap<>();
        body.put("status", HttpStatus.NOT_FOUND.value());
        body.put("error", "Not Found");
        body.put("message", "找不到對應的訂單，請確認訂單 ID 是否正確。");
        body.put("details", ex.getMessage());
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(body);
    }

    /**
     * 處理無效業務狀態的異常 (IllegalStateException)。
     * 當訂單違反商業邏輯規則時（例如：在已付款或已取消的狀態下再次執行付款或取消操作），
     * 聚合根會拋出此異常。本方法將其轉化為 HTTP 400 Bad Request 回應。
     *
     * @param ex 狀態機異常
     * @return 包含業務錯誤說明之 ResponseEntity
     */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalState(IllegalStateException ex) {
        Map<String, Object> body = new HashMap<>();
        body.put("status", HttpStatus.BAD_REQUEST.value());
        body.put("error", "Bad Request");
        body.put("message", "訂單業務規則限制: " + ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    /**
     * 處理 Axon 指令執行期間的包裝異常 (CommandExecutionException)。
     * <p>
     * 當藉由 CommandGateway 發送的指令在後台處理器執行失敗時，Axon 會將底層的真實例外封裝進 CommandExecutionException。
     * 為了避免向客戶端噴出過多系統實作細節，本方法會：
     * 1. 拆解錯誤訊息，判斷底層是否因為遠端 Axon Server 找不到對應 Aggregate (例如 gRPC 調用回傳找不到聚合)。
     * 2. 若是，則改以 HTTP 404 返回；若非，則以 HTTP 500 Internal Server Error 封裝通用指令失敗訊息。
     * </p>
     *
     * @param ex 指令執行異常
     * @return 統一包裝後的 ResponseEntity 錯誤回應
     */
    @ExceptionHandler(CommandExecutionException.class)
    public ResponseEntity<Map<String, Object>> handleCommandExecution(CommandExecutionException ex) {
        Map<String, Object> body = new HashMap<>();
        
        // 檢查是否為 AggregateNotFoundException 引起的遠端異常
        if (ex.getMessage() != null && ex.getMessage().contains("The aggregate was not found")) {
            body.put("status", HttpStatus.NOT_FOUND.value());
            body.put("error", "Not Found");
            body.put("message", "找不到對應的訂單，請確認訂單 ID 是否正確。");
            body.put("details", ex.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(body);
        }

        body.put("status", HttpStatus.INTERNAL_SERVER_ERROR.value());
        body.put("error", "Command Execution Error");
        body.put("message", "指令執行失敗: " + ex.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
    }
}
