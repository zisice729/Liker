package com.example.liker.mq.consumer;

import com.example.liker.entity.LikeCompensate;
import com.example.liker.entity.LikesRecord;
import com.example.liker.mapper.LikeCompensateMapper;
import com.example.liker.mapper.LikesRecordMapper;
import com.example.liker.mq.dto.LikeKafkaMsg;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka消息消费者
 * 负责消费点赞消息并持久化到MySQL
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LikeConsumer {

    private final LikesRecordMapper recordMapper;
    private final LikeCompensateMapper compensateMapper;
    private final ObjectMapper objectMapper;

    /**
     * 监听点赞消息Topic
     * 手动提交offset，消费失败的消息写入补偿表
     *
     * @param records Kafka消息记录列表
     * @param ack 消息确认对象
     */
    @KafkaListener(topics = "topic_like")
    public void listen(org.apache.kafka.clients.consumer.ConsumerRecords<String, LikeKafkaMsg> records, 
                       Acknowledgment ack) {
        for (var record : records) {
            LikeKafkaMsg msg = record.value();
            try {
                // 构建持久化记录
                LikesRecord po = new LikesRecord();
                po.setObjId(msg.getObjId());
                po.setUserId(msg.getUserId());
                po.setStatus(msg.getAction());
                recordMapper.insertOrUpdate(po);
            } catch (Exception e) {
                // 消费失败，写入补偿表
                LikeCompensate compensate = new LikeCompensate();
                try {
                    compensate.setMsgBody(objectMapper.writeValueAsString(msg));
                } catch (Exception ex) {
                    compensate.setMsgBody(msg.toString());
                }
                compensate.setRetryTimes(0);
                compensate.setStatus(1);
                compensate.setErrorMsg(e.getMessage());
                compensateMapper.insert(compensate);
            }
        }
        // 手动提交offset
        ack.acknowledge();
    }
}