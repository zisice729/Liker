package com.example.liker.util;

import com.example.liker.constant.CommonConst;
import org.springframework.stereotype.Component;

/**
 * 分片工具类
 * 用于计算用户ID对应的分片索引，实现点赞数据的分片存储
 */
@Component
public class ShardUtil {

    /**
     * 根据用户ID计算分片索引
     * @param userId 用户ID
     * @return 分片索引（0 ~ SHARD_COUNT-1）
     */
    public int getShardIndex(Long userId) {
        return (int) (userId % CommonConst.SHARD_COUNT);
    }
}