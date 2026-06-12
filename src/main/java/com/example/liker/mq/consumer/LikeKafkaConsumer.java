
package com.example.liker.mq.consumer;

import com.example.liker.mq.msg.LikeMqMsg;
import com.example.liker.repository.do.UserLikeDO;
import com.example.liker.repository.entity.LikeMsgCompensate;
import com.example.liker.repository.mapper.LikeMsgCompensateMapper;
import com.example.liker.repository.mapper.UserLikeMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

/**
 * Kafka消费者 - 处理点赞消息
 * 包含一级兜底：内存指数退避重试
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LikeKafkaConsumer {

    private final UserLikeMapper userLikeMapper;
    private final LikeMsgCompensateMapper compensateMapper;
    private final ObjectMapper objectMapper;

    @Value("${liker.kafka.retry.max-attempts:4}")
    private int maxRetryAttempts;

    @Value("${liker.kafka.retry.initial-delay-ms:200}")
    private long initialDelayMs;

    @Value("${liker.kafka.retry.multiplier:2.0}")
    private double retryMultiplier;

    @Value("${liker.kafka.retry.max-delay-ms:5000}")
    private long maxDelayMs;

    /**
     * 消费点赞消息
     */
    @KafkaListener(topics = "${liker.kafka.topic:topic_like}", 
                   groupId = "${spring.kafka.consumer.group-id:like-consumer-group}")
    public void consume(LikeMqMsg msg) {
        log.debug("收到Kafka消息: {}", msg);

        String msgBody;
        try {
            msgBody = objectMapper.writeValueAsString(msg);
        } catch (JsonProcessingException e) {
            log.error("消息序列化失败: {}", e.getMessage());
            return;
        }

        String uniqueKey = UUID.randomUUID().toString();

        // 一级兜底：指数退避重试
        boolean success = processWithRetry(msg, msgBody, uniqueKey);

        if (!success) {
            // 一级重试失败，写入补偿表（二级兜底）
            writeToCompensateTable(msgBody, uniqueKey);
        }
    }

    /**
     * 带指数退避重试的消息处理
     */
    private boolean processWithRetry(LikeMqMsg msg, String msgBody, String uniqueKey) {
        int attempt = 0;
        long delay = initialDelayMs;

        while (attempt < maxRetryAttempts) {
            try {
                return processMessage(msg);
            } catch (Exception e) {
                attempt++;
                log.warn("消息处理失败，第{}次重试: msg={}, error={}", attempt, msg, e.getMessage());

                if (attempt >= maxRetryAttempts) {
                    log.error("消息处理达到最大重试次数，即将写入补偿表: msg={}", msg);
                    return false;
                }

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("重试等待被中断");
                    return false;
                }

                delay = Math.min((long) (initialDelayMs * Math.pow(retryMultiplier, attempt)), maxDelayMs);
            }
        }
        return false;
    }

    /**
     * 处理消息主逻辑
     */
    private boolean processMessage(LikeMqMsg msg) {
        UserLikeDO userLike = UserLikeDO.builder()
                .bizType(msg.getBizType())
                .bizId(msg.getBizId())
                .userId(msg.getUserId())
                .operateType(msg.getOperateType())
                .operateTime(LocalDateTime.ofInstant(
                        java.time.Instant.ofEpochMilli(msg.getOperateTs()),
                        ZoneId.systemDefault()))
                .build();

        userLikeMapper.insertOrUpdate(userLike);
        log.debug("消息处理成功: bizType={}, bizId={}, userId={}",
                msg.getBizType(), msg.getBizId(), msg.getUserId());
        return true;
    }

    /**
     * 写入补偿表（二级兜底）
     */
    private void writeToCompensateTable(String msgBody, String uniqueKey) {
        try {
            LikeMsgCompensate compensate = LikeMsgCompensate.builder()
                    .msgUniqueKey(uniqueKey)
                    .msgBody(msgBody)
                    .retryCount(0)
                    .maxRetry(8)
                    .status(1)
                    .errorMsg("Kafka消费失败，已达最大重试次数")
                    .build();
            compensateMapper.insert(compensate);
            log.warn("消息处理失败，已写入补偿表: uniqueKey={}", uniqueKey);
        } catch (Exception e) {
            log.error("写入补偿表失败: {}", e.getMessage(), e);
        }
    }
}
