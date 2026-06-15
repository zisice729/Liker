package com.example.liker.mq.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * Kafka点赞消息体
 * 用于异步传递点赞操作信息到MySQL持久化
 */
@Data
public class LikeKafkaMsg implements Serializable {

    /**
     * 业务对象ID（文章ID/评论ID）
     */
    private Long objId;

    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 操作类型：1-点赞，0-取消点赞
     */
    private Integer action;

    /**
     * 操作时间戳
     */
    private Long timestamp;
}