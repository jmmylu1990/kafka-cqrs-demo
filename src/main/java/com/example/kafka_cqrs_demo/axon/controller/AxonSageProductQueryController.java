package com.example.kafka_cqrs_demo.axon.controller;

import com.example.kafka_cqrs_demo.axon.dto.ProductStockDto;
import com.example.kafka_cqrs_demo.axon.service.ProductQueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 商品庫存查詢端 API 控制器 (Product Query Controller)
 * <p>
 * 重構後已符合 SOLID 原則，將底層的 Cache-Aside 讀寫、DCL 防禦鎖及指標統計邏輯
 * 抽離至 ProductQueryService 中，本控制器僅負責處理 HTTP 請求的路由與狀態碼對應。
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("/axonsaga/api/products")
public class AxonSageProductQueryController {

    private final ProductQueryService productQueryService;

    public AxonSageProductQueryController(ProductQueryService productQueryService) {
        this.productQueryService = productQueryService;
    }

    /**
     * 查詢特定商品的可用庫存與預留庫存。
     *
     * @param productId 商品 ID
     * @return 包含庫存資訊的響應實體，若不存在則回傳 404
     */
    @GetMapping("/{productId}/stock")
    public ResponseEntity<ProductStockDto> getProductStock(@PathVariable String productId) {
        log.info("查詢商品庫存請求，ID: {}", productId);
        try {
            ProductStockDto dto = productQueryService.getProductStock(productId);
            return ResponseEntity.ok(dto);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build();
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
