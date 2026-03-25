package com.lengye.facade.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Map;
import java.util.UUID;

/**
 * @author lengye
 */
@Data
@NoArgsConstructor
public class NotificationRequest {
    // 默认生成，也可由业务传入
    private String messageId = UUID.randomUUID().toString();

    @NotBlank
    private String url;

    // 默认 POST
    @NotBlank
    private String method = "POST";

    private Map<String, String> headers;

    // 字符串格式，可以是 JSON 或表单等
    private String body;

    // 最大重试次数，可配置
    private int maxRetries = 3;

    /// / 当前重试次数，由系统维护，业务传入会被覆盖
    private int retryCount = 0;
}