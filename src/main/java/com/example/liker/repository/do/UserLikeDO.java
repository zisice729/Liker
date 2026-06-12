
package com.example.liker.repository.do;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 用户点赞DO实体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("user_like")
public class UserLikeDO {

    @TableId(type = IdType.AUTO)
    private Long id;

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
     * 操作时间
     */
    private LocalDateTime operateTime;

    /**
     * 创建时间
     */
    private LocalDateTime createTime;

    /**
     * 更新时间
     */
    private LocalDateTime updateTime;
}
