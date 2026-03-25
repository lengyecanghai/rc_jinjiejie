package com.lengye.yy.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMQConfig {

    // 交换机
    public static final String PENDING_EXCHANGE = "pending.exchange";
    public static final String RETRY_EXCHANGE = "retry.exchange";
    public static final String DLX_EXCHANGE = "dead.letter.exchange";

    // 队列
    public static final String PENDING_QUEUE = "pending.queue";
    public static final String RETRY_QUEUE = "retry.queue";
    public static final String DLQ = "dead.letter.queue";

    // Routing Keys
    public static final String PENDING_ROUTING_KEY = "pending";
    public static final String RETRY_ROUTING_KEY = "retry";
    public static final String DLX_ROUTING_KEY = "dead.letter";

    @Bean
    public DirectExchange pendingExchange() {
        return new DirectExchange(PENDING_EXCHANGE, true, false);
    }

    @Bean
    public DirectExchange retryExchange() {
        return new DirectExchange(RETRY_EXCHANGE, true, false);
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(DLX_EXCHANGE, true, false);
    }

    // 主队列：绑定到 pending 交换机
    @Bean
    public Queue pendingQueue() {
        Map<String, Object> args = new HashMap<>();
        // 设置死信交换机
        args.put("x-dead-letter-exchange", DLX_EXCHANGE);
        // 设置死信路由键（可选）
        args.put("x-dead-letter-routing-key", DLX_ROUTING_KEY);
        return QueueBuilder.durable(PENDING_QUEUE).withArguments(args).build();
    }

    @Bean
    public Binding pendingBinding() {
        return BindingBuilder.bind(pendingQueue())
                .to(pendingExchange())
                .with(PENDING_ROUTING_KEY);
    }

    // 重试队列：绑定到 retry 交换机，并设置死信
    @Bean
    public Queue retryQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", DLX_EXCHANGE);
        args.put("x-dead-letter-routing-key", DLX_ROUTING_KEY);
        return QueueBuilder.durable(RETRY_QUEUE).withArguments(args).build();
    }

    @Bean
    public Binding retryBinding() {
        return BindingBuilder.bind(retryQueue())
                .to(retryExchange())
                .with(RETRY_ROUTING_KEY);
    }

    // 死信队列
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DLQ).build();
    }

    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue())
                .to(deadLetterExchange())
                .with(DLX_ROUTING_KEY);
    }

    // 消息转换器（JSON）
    @Bean
    public Jackson2JsonMessageConverter jackson2JsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // 配置 RabbitTemplate 使用 ConfirmCallback 和 ReturnCallback（可选）
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jackson2JsonMessageConverter());
        // 开启 publisher confirm
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                // 记录发送失败，可记录到日志或数据库
                System.err.println("消息发送失败: " + cause);
            }
        });
        return template;
    }

    // 配置消费者容器工厂，使用手动 ACK
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jackson2JsonMessageConverter());
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL); // 手动确认
        return factory;
    }
}