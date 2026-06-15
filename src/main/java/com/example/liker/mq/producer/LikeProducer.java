package com.example.liker.mq.producer;

import com.example.liker.mq.dto.LikeKafkaMsg;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka消息生产者
 * 负责发送点赞消息到Kafka Topic
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LikeProducer {

    /**
     * Kafka Topic名称
     */
    private static final String TOPIC = "topic_like";

    private final KafkaTemplate<String, LikeKafkaMsg> kafkaTemplate;

    /**
     * 发送点赞消息
     *
     * @param msg 点赞消息体
     */
    public void sendMsg(LikeKafkaMsg msg) {
        kafkaTemplate.send(TOPIC, msg).addCallback(
                result -> log.info("Kafka消息发送成功"),
                ex -> log.error("Kafka消息发送失败", ex)
        );
    }
}