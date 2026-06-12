
package com.example.liker.mq.msg;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Kafka消息体DTO（MQ内部传输专用）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LikeMqMsg implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 业务类型
     */
    private Integer bizType;

    /**
     * 业务ID
     */
    private Long bizId;

    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 操作类型：1-点赞，2-取消点赞
     */
    private Integer operateType;

    /**
     * 操作时间戳
     */
    private Long operateTs;
}
