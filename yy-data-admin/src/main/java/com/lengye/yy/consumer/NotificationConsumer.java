package com.lengye.yy.consumer;

import com.lengye.facade.model.NotificationRequest;
import com.lengye.yy.config.RabbitMQConfig;
import com.lengye.yy.exception.DeliveryException;
import com.lengye.yy.service.HttpDeliveryService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lengye
 */
@Slf4j
@Component
public class NotificationConsumer {

    @Autowired
    private HttpDeliveryService httpDeliveryService;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    /**
     * 从主队列消费消息，进行投递(AI生成的内容)，如果投递失败，根据重试次数决定是进入重试队列还是死信队列
     */
//    @RabbitListener(queues = RabbitMQConfig.PENDING_QUEUE, containerFactory = "rabbitListenerContainerFactory")
//    public void consume(NotificationRequest request, Channel channel,
//                        @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
//        log.info("收到消息: messageId={}, retryCount={}", request.getMessageId(), request.getRetryCount());
//        try {
//            httpDeliveryService.deliver(request);
//            // 投递成功，手动 ACK
//            channel.basicAck(deliveryTag, false);
//        } catch (DeliveryException e) {
//            log.warn("投递失败: messageId={}, error={}", request.getMessageId(), e.getMessage());
//            // 判断是否需要重试
//            int currentRetry = request.getRetryCount();
//            if (currentRetry < request.getMaxRetries()) {
//                // 增加重试次数
//                request.setRetryCount(currentRetry + 1);
//                // 计算延迟时间：指数退避，例如 1s, 2s, 4s...
//                long delayMillis = (long) Math.pow(2, currentRetry) * 1000;
//                // 发送到重试交换机，并设置 TTL
//                sendToRetryExchange(request, delayMillis);
//                // 确认原消息（已被消费，不需要再保留）
//                channel.basicAck(deliveryTag, false);
//                log.info("消息进入重试队列: messageId={}, retryCount={}, delay={}ms",
//                        request.getMessageId(), request.getRetryCount(), delayMillis);
//            } else {
//                // 超过最大重试次数，发送到死信队列（直接 NACK 并让其进入死信？或者手动发到死信交换机）
//                // 我们选择直接发送到死信交换机，然后 ACK 原消息
//                sendToDeadLetter(request, e.getMessage());
//                channel.basicAck(deliveryTag, false);
//                log.error("消息最终失败，已进入死信队列: messageId={}", request.getMessageId());
//            }
//        } catch (Exception ex) {
//            // 其他异常（如序列化），记录并拒绝消息，不重试（避免无限循环）
//            log.error("消费异常，拒绝消息: messageId={}", request.getMessageId(), ex);
//            // 不重新入队
//            channel.basicReject(deliveryTag, false);
//        }
//    }

    /**
     * 发送到重试交换机，带 TTL 延迟
     */
    private void sendToRetryExchange(NotificationRequest request, long delayMillis) {
        // 注意：RabbitMQ 的延迟消息需要队列或消息级别的 TTL，这里使用消息级别的 TTL
        // 创建消息属性，设置 expiration（毫秒）
        Message message = rabbitTemplate.getMessageConverter().toMessage(request, null);
        message.getMessageProperties().setExpiration(String.valueOf(delayMillis));
        // 发送到重试交换机，路由键为 retry
        rabbitTemplate.send(RabbitMQConfig.RETRY_EXCHANGE, RabbitMQConfig.RETRY_ROUTING_KEY, message);
    }

    /**
     * 发送到死信队列（可以直接使用死信交换机）
     */
    private void sendToDeadLetter(NotificationRequest request, String reason) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("failureReason", reason);
        headers.put("failedAt", Instant.now().toString());
        Message message = rabbitTemplate.getMessageConverter().toMessage(request, null);
        message.getMessageProperties().setHeaders(headers);
        rabbitTemplate.send(RabbitMQConfig.DLX_EXCHANGE, RabbitMQConfig.DLX_ROUTING_KEY, message);
    }


    /**
     * 人为的变更重试队列的消费者，模拟重试逻辑，验证重试机制是否正常工作
     * 需要单独一个消费者监听 retry.queue，逻辑与主消费者相同：尝试投递，成功则 ACK，
     * 失败则根据重试次数决定继续重试（再次进入重试队列）或进入死信。为了避免代码重复，
     * 我们可以让 retry.queue 的消费者直接调用同一个 consume 方法吗？不行，因为 consume 已经绑定到主队列。
     * 我们需要创建一个新的方法监听重试队列，但逻辑几乎一样。我们可以抽取公共投递逻辑到一个方法中。
     * 修改消费者类，新增一个监听重试队列的方法，复用投递逻辑：
     *
     */
    @RabbitListener(queues = RabbitMQConfig.RETRY_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void retryConsume(NotificationRequest request, Channel channel,
                             @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        // 与主队列消费逻辑完全一样，直接调用统一处理方法
        processDelivery(request, channel, deliveryTag);
    }

    /**
     * 统一投递处理
     */
    private void processDelivery(NotificationRequest request, Channel channel, long deliveryTag) throws IOException {
        log.info("处理投递: messageId={}, retryCount={}", request.getMessageId(), request.getRetryCount());
        try {
            httpDeliveryService.deliver(request);
            channel.basicAck(deliveryTag, false);
        } catch (DeliveryException e) {
            log.warn("投递失败: messageId={}, error={}", request.getMessageId(), e.getMessage());
            int currentRetry = request.getRetryCount();
            if (currentRetry < request.getMaxRetries()) {
                request.setRetryCount(currentRetry + 1);
                long delayMillis = (long) Math.pow(2, currentRetry) * 1000;
                sendToRetryExchange(request, delayMillis);
                channel.basicAck(deliveryTag, false);
            } else {
                sendToDeadLetter(request, e.getMessage());
                channel.basicAck(deliveryTag, false);
            }
        } catch (Exception ex) {
            log.error("消费异常，拒绝消息: messageId={}", request.getMessageId(), ex);
            channel.basicReject(deliveryTag, false);
        }
    }
}