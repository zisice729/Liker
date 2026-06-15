package com.example.liker.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 点赞归档表实体
 * 用于持久化存储点赞记录，实现冷数据归档
 */
@Data
@TableName("likes_record")
public class LikesRecord {

    /**
     * 主键ID
     */
    @TableId(type = IdType.AUTO)
    private Long id;

    /**
     * 业务对象ID（文章ID/评论ID）
     */
    private Long objId;

    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 点赞状态：1-点赞，0-取消点赞
     */
    private Integer status;

    /**
     * 创建时间
     */
    private LocalDateTime createTime;
}