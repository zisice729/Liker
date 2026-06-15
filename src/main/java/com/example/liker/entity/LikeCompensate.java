package com.example.liker.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Kafka消费补偿表实体
 * 用于存储消费失败的消息，支持重试和人工处理
 */
@Data
@TableName("like_compensate")
public class LikeCompensate {

    /**
     * 主键ID
     */
    @TableId(type = IdType.AUTO)
    private Long id;

    /**
     * 原始Kafka消息体（JSON格式）
     */
    private String msgBody;

    /**
     * 已重试次数
     */
    private Integer retryTimes;

    /**
     * 状态：1-待重试，2-成功，3-重试耗尽待人工处理
     */
    private Integer status;

    /**
     * 异常信息
     */
    private String errorMsg;

    /**
     * 创建时间
     */
    private LocalDateTime createTime;

    /**
     * 更新时间
     */
    private LocalDateTime updateTime;
}