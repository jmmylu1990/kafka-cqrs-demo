package com.example.kafka_cqrs_demo.axon.service;

import com.example.kafka_cqrs_demo.axon.dto.ProductStockDto;

/**
 * 商品庫存查詢服務介面 (Product Query Service Interface)
 */
public interface ProductQueryService {
    /**
     * 查詢指定商品之可用庫存與預留庫存。
     *
     * @param productId 商品識別碼
     * @return 包含庫存資訊的 DTO
     */
    ProductStockDto getProductStock(String productId);
}
