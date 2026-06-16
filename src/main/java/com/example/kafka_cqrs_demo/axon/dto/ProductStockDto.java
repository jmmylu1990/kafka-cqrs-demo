package com.example.kafka_cqrs_demo.axon.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 商品可用庫存與預留庫存資料傳輸物件 (Product Stock DTO)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductStockDto {
    private String productId;
    private int stock;
    private int reservedStock;
}
