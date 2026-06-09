package com.example.kafka_cqrs_demo;

import com.example.kafka_cqrs_demo.axon.repository.AxonInventoryRepository;
import com.example.kafka_cqrs_demo.axon.repository.AxonStockReservationRepository;
import com.example.kafka_cqrs_demo.axon.service.InventoryReconciliationJob;
import com.example.kafka_cqrs_demo.axon.service.InventoryReconciliationJob.ReconciliationReport;
import com.example.kafka_cqrs_demo.entity.AxonInventoryEntity;
import com.example.kafka_cqrs_demo.entity.AxonStockReservationEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class InventoryReconciliationJobTest {

    private AxonInventoryRepository inventoryRepository;
    private AxonStockReservationRepository reservationRepository;
    private RedissonClient redissonClient;
    private InventoryReconciliationJob job;

    @BeforeEach
    void setUp() {
        inventoryRepository = mock(AxonInventoryRepository.class);
        reservationRepository = mock(AxonStockReservationRepository.class);
        redissonClient = mock(RedissonClient.class);
        job = new InventoryReconciliationJob(inventoryRepository, reservationRepository, redissonClient);
    }

    @Test
    void testReconcileStockDrift() throws InterruptedException {
        // Given
        AxonInventoryEntity mysqlInv = new AxonInventoryEntity("PROD-001", 100, 10);
        when(inventoryRepository.findAll()).thenReturn(List.of(mysqlInv));
        when(reservationRepository.findAll()).thenReturn(Collections.emptyList());

        RLock mockLock = mock(RLock.class);
        when(mockLock.tryLock(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(mockLock.isHeldByCurrentThread()).thenReturn(true);
        when(redissonClient.getLock("lock:product:PROD-001")).thenReturn(mockLock);

        RBucket<String> mockStockBucket = mock(RBucket.class);
        when(mockStockBucket.get()).thenReturn("80"); // Drift
        doReturn(mockStockBucket).when(redissonClient).getBucket("product:PROD-001:stock", StringCodec.INSTANCE);

        RBucket<String> mockReservedBucket = mock(RBucket.class);
        when(mockReservedBucket.get()).thenReturn("10"); // Correct
        doReturn(mockReservedBucket).when(redissonClient).getBucket("product:PROD-001:reserved", StringCodec.INSTANCE);

        RBucket<String> mockUpdatedAtBucket = mock(RBucket.class);
        when(mockUpdatedAtBucket.get()).thenReturn(null); // No recent update
        doReturn(mockUpdatedAtBucket).when(redissonClient).getBucket("product:PROD-001:updatedAt", StringCodec.INSTANCE);

        RKeys mockKeys = mock(RKeys.class);
        when(mockKeys.getKeysByPattern(anyString())).thenReturn(Collections.emptyList());
        when(redissonClient.getKeys()).thenReturn(mockKeys);

        // When
        ReconciliationReport report = job.reconcile();

        // Then
        assertTrue(report.isSuccess());
        assertEquals(1, report.getProductsAudited());
        assertEquals(1, report.getStockDriftsDetected());
        assertEquals(1, report.getStockDriftsCorrected());
        verify(mockStockBucket).set("100");
        verify(mockLock).unlock();
    }

    @Test
    void testReconcileReservationDrift() throws InterruptedException {
        // Given
        when(inventoryRepository.findAll()).thenReturn(Collections.emptyList());

        AxonStockReservationEntity mysqlRes = new AxonStockReservationEntity("order-1", "PROD-001", 5, "RESERVED", LocalDateTime.now());
        when(reservationRepository.findAll()).thenReturn(List.of(mysqlRes));

        RMap<String, String> mockMap = mock(RMap.class);
        when(mockMap.isExists()).thenReturn(true);
        when(mockMap.get("productId")).thenReturn("PROD-001");
        when(mockMap.get("quantity")).thenReturn("5");
        when(mockMap.get("status")).thenReturn("COMPLETED"); // Drift! Should be RESERVED in Redis
        doReturn(mockMap).when(redissonClient).getMap("order:order-1:reservation", StringCodec.INSTANCE);

        RKeys mockKeys = mock(RKeys.class);
        when(mockKeys.getKeysByPattern("order:*:reservation")).thenReturn(List.of("order:order-1:reservation"));
        when(redissonClient.getKeys()).thenReturn(mockKeys);

        // When
        ReconciliationReport report = job.reconcile();

        // Then
        assertTrue(report.isSuccess());
        assertEquals(1, report.getReservationsAudited());
        assertEquals(1, report.getReservationDriftsDetected());
        assertEquals(1, report.getReservationDriftsCorrected());
        verify(mockMap).put("status", "RESERVED");
    }
}
