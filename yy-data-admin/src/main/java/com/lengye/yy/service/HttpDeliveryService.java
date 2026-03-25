package com.lengye.yy.service;

import com.lengye.facade.model.NotificationRequest;
import com.lengye.yy.exception.DeliveryException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
/**
 * @author lengye
 */
@Slf4j
@Service
public class HttpDeliveryService {

    private final RestTemplate restTemplate;

    public HttpDeliveryService() {
        // 设置连接超时和读取超时
        this.restTemplate = new RestTemplate();
        // 可自定义 ClientHttpRequestFactory 设置超时，这里简略
    }

    /**
     * 执行 HTTP 请求
     * @param request 通知请求
     * @throws DeliveryException 当请求失败时抛出
     */
    public void deliver(NotificationRequest request) throws DeliveryException {
        HttpHeaders headers = new HttpHeaders();
        if (request.getHeaders() != null) {
            request.getHeaders().forEach(headers::add);
        }

        HttpEntity<String> entity = new HttpEntity<>(request.getBody(), headers);

        try {
            HttpMethod method = HttpMethod.valueOf(request.getMethod().toUpperCase());
            ResponseEntity<String> response = restTemplate.exchange(
                    request.getUrl(),
                    method,
                    entity,
                    String.class
            );

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("投递成功: messageId={}, url={}", request.getMessageId(), request.getUrl());
            } else {
                // 非 2xx 也视为失败，需要重试
                throw new DeliveryException("HTTP 状态码异常: " + response.getStatusCode());
            }
        } catch (RestClientException e) {
            throw new DeliveryException("网络异常: " + e.getMessage(), e);
        }
    }
}