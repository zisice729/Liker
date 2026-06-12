
package com.example.liker.mq.producer;

import com.example.liker.common.constants.LikeConstant;
import com.example.liker.mq.msg.LikeMqMsg;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * Kafka生产者服务实现类
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaProducerServiceImpl {

    private final KafkaTemplate<String, LikeMqMsg> kafkaTemplate;

    /**
     * 发送点赞消息到Kafka
     */
    public void sendMsg(LikeMqMsg msg) {
        String key = msg.getBizType() + "_" + msg.getBizId();

        ListenableFuture<SendResult<String, LikeMqMsg>> future = kafkaTemplate.send(LikeConstant.KAFKA_TOPIC_LIKE, key, msg);

        future.addCallback(new ListenableFutureCallback<SendResult<String, LikeMqMsg>>() {
            @Override
            public void onSuccess(SendResult<String, LikeMqMsg> result) {
                log.debug("Kafka消息发送成功: topic={}, key={}, offset={}",
                        LikeConstant.KAFKA_TOPIC_LIKE, key, result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("Kafka消息发送失败: topic={}, key={}, error={}", LikeConstant.KAFKA_TOPIC_LIKE, key, ex.getMessage(), ex);
            }
        });
    }
}
