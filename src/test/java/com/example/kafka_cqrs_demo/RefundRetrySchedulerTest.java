package com.example.kafka_cqrs_demo;

import com.example.kafka_cqrs_demo.axon.event.WalletRefundedEvent;
import com.example.kafka_cqrs_demo.axon.repository.AxonRefundRetryTaskRepository;
import com.example.kafka_cqrs_demo.axon.service.RefundRetryScheduler;
import com.example.kafka_cqrs_demo.entity.AxonRefundRetryTaskEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import lombok.Data;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.axonframework.eventhandling.gateway.EventGateway;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class RefundRetrySchedulerTest {

    private AxonRefundRetryTaskRepository refundRetryTaskRepository;
    private EventGateway eventGateway;
    private RestTemplate restTemplate;
    private RefundRetryScheduler scheduler;

    @BeforeEach
    void setUp() throws Exception {
        refundRetryTaskRepository = mock(AxonRefundRetryTaskRepository.class);
        eventGateway = mock(EventGateway.class);
        restTemplate = mock(RestTemplate.class);
        scheduler = new RefundRetryScheduler(refundRetryTaskRepository, eventGateway);

        // 使用反射將 Mocked RestTemplate 注入到 Scheduler 中，以避免發起真實的 HTTP 請求
        Field restTemplateField = RefundRetryScheduler.class.getDeclaredField("restTemplate");
        restTemplateField.setAccessible(true);
        restTemplateField.set(scheduler, restTemplate);
    }

    @Test
    void testRetryPendingRefunds_Success() throws Exception {
        // Given
        AxonRefundRetryTaskEntity task = new AxonRefundRetryTaskEntity(
                "task-1", "user-123", "order-123", 500L, 0, "PENDING",
                "退款失敗", LocalDateTime.now().minusMinutes(1), LocalDateTime.now().minusMinutes(1), LocalDateTime.now()
        );
        when(refundRetryTaskRepository.findByStatusAndNextRetryTimeBefore(eq("PENDING"), any(LocalDateTime.class)))
                .thenReturn(List.of(task));

        // 直接建構已改為 public 的 PaymentApiResponse
        RefundRetryScheduler.PaymentApiResponse mockResponse = new RefundRetryScheduler.PaymentApiResponse();
        mockResponse.setSuccess(true);

        ResponseEntity<Object> responseEntity = new ResponseEntity<>(mockResponse, HttpStatus.OK);
        when(restTemplate.postForEntity(anyString(), any(), any())).thenReturn((ResponseEntity) responseEntity);

        // When
        scheduler.retryPendingRefunds();

        // Then
        assertEquals("SUCCESS", task.getStatus());
        verify(refundRetryTaskRepository).save(task);
        verify(eventGateway).publish(any(WalletRefundedEvent.class));
    }

    @Test
    void testRetryPendingRefunds_Failed_IncrementRetryCount() {
        // Given
        AxonRefundRetryTaskEntity task = new AxonRefundRetryTaskEntity(
                "task-1", "user-123", "order-123", 500L, 0, "PENDING",
                "退款失敗", LocalDateTime.now().minusMinutes(1), LocalDateTime.now().minusMinutes(1), LocalDateTime.now()
        );
        when(refundRetryTaskRepository.findByStatusAndNextRetryTimeBefore(eq("PENDING"), any(LocalDateTime.class)))
                .thenReturn(List.of(task));

        // 模擬外部 API 拋出連線異常
        when(restTemplate.postForEntity(anyString(), any(), any()))
                .thenThrow(new RestClientException("網路連線超時"));

        // When
        scheduler.retryPendingRefunds();

        // Then
        assertEquals("PENDING", task.getStatus());
        assertEquals(1, task.getRetryCount());
        assertEquals("網路連線超時", task.getLastErrorMessage());
        assertTrue(task.getNextRetryTime().isAfter(LocalDateTime.now()));
        verify(refundRetryTaskRepository).save(task);
        verify(eventGateway, never()).publish(any(WalletRefundedEvent.class));
    }

    @Test
    void testRetryPendingRefunds_Failed_ReachedMaxAttempts() {
        // Given
        // 已重試 4 次，當前重試將是第 5 次，預計重試後會改為 FAILED
        AxonRefundRetryTaskEntity task = new AxonRefundRetryTaskEntity(
                "task-1", "user-123", "order-123", 500L, 4, "PENDING",
                "退款失敗", LocalDateTime.now().minusMinutes(1), LocalDateTime.now().minusMinutes(1), LocalDateTime.now()
        );
        when(refundRetryTaskRepository.findByStatusAndNextRetryTimeBefore(eq("PENDING"), any(LocalDateTime.class)))
                .thenReturn(List.of(task));

        // 模擬外部 API 拋出伺服器 500 錯誤例外
        when(restTemplate.postForEntity(anyString(), any(), any()))
                .thenThrow(new RestClientException("HTTP 500 內部伺服器錯誤"));

        // When
        scheduler.retryPendingRefunds();

        // Then
        assertEquals("FAILED", task.getStatus());
        assertEquals(5, task.getRetryCount());
        assertEquals("HTTP 500 內部伺服器錯誤", task.getLastErrorMessage());
        verify(refundRetryTaskRepository).save(task);
        verify(eventGateway, never()).publish(any(WalletRefundedEvent.class));
    }
}
