package com.lengye.yy.service;


import com.lengye.facade.model.NotificationRequest;
import com.lengye.yy.config.RabbitMQConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author lengye
 */
@Slf4j
@Service
public class NotificationService {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    public void send(NotificationRequest request) {
        // 保证 retryCount 初始为 0
        request.setRetryCount(0);
        // 使用 RabbitTemplate 发送到主交换机
        rabbitTemplate.convertAndSend(RabbitMQConfig.PENDING_EXCHANGE, RabbitMQConfig.PENDING_ROUTING_KEY, request);
        log.info("消息已发送到队列: messageId={}", request.getMessageId());
    }
}