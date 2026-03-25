package com.lengye.yy.controller;


import com.lengye.facade.model.NotificationRequest;
import com.lengye.yy.service.NotificationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * @author lengye
 */
@Slf4j
@RestController
@RequestMapping("/homework")
@Tag(name = "HomeWorkController", description = "HomeWorkController")
public class HomeWorkController {

    @Autowired
    private NotificationService notificationService;

    @Operation(summary = "测试消息队列入口")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "消息体"),
    })
    @PostMapping
    public ResponseEntity<Void> notify(@Valid @RequestBody NotificationRequest request) {
        // 如果业务没有传入 messageId，则自动生成（已在模型默认生成）
        notificationService.send(request);
        log.info("接收通知请求: messageId={}, url={}", request.getMessageId(), request.getUrl());
        // 202 Accepted
        return ResponseEntity.accepted().build();
    }

}
