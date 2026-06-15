package com.example.liker.mq.consumer;

import com.example.liker.entity.LikeCompensate;
import com.example.liker.entity.LikesRecord;
import com.example.liker.mapper.LikeCompensateMapper;
import com.example.liker.mapper.LikesRecordMapper;
import com.example.liker.mq.dto.LikeKafkaMsg;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka消息消费者
 * 负责消费点赞消息并持久化到MySQL
 */
@Component
public class LikeConsumer {

    private static final Logger log = LoggerFactory.getLogger(LikeConsumer.class);

    private final LikesRecordMapper recordMapper;
    private final LikeCompensateMapper compensateMapper;
    private final ObjectMapper objectMapper;

    public LikeConsumer(LikesRecordMapper recordMapper, LikeCompensateMapper compensateMapper, ObjectMapper objectMapper) {
        this.recordMapper = recordMapper;
        this.compensateMapper = compensateMapper;
        this.objectMapper = objectMapper;
    }

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
        for (ConsumerRecord<String, LikeKafkaMsg> record : records) {
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