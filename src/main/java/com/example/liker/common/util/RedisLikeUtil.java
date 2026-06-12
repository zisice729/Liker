
package com.example.liker.common.util;

import com.example.liker.config.RedisConfig;
import com.example.liker.common.constants.LikeConstant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * Redis点赞操作工具类
 * 基于Redisson封装，提供点赞/取消点赞、获取点赞数、检查用户点赞状态等操作
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisLikeUtil {

    private final RedissonClient redissonClient;

    /**
     * 点赞/取消点赞Lua脚本
     * 返回值：1-点赞成功，0-取消点赞成功
     */
    private static final String LIKE_LUA_SCRIPT = 
        "local key = KEYS[1]\n" +
        "local userId = ARGV[1]\n" +
        "local ts = ARGV[2]\n" +
        "local existed = redis.call('ZSCORE', key, userId)\n" +
        "if existed then\n" +
        "    redis.call('ZREM', key, userId)\n" +
        "    return 0\n" +
        "else\n" +
        "    redis.call('ZADD', key, ts, userId)\n" +
        "    return 1\n" +
        "end";

    /**
     * 执行点赞操作（原子操作）
     * 
     * @param bizType 业务类型
     * @param bizId 业务ID
     * @param userId 用户ID
     * @return 1-点赞成功，0-取消点赞成功，null-操作失败
     */
    public Long like(Integer bizType, Long bizId, Long userId) {
        try {
            String likeKey = RedisConfig.buildLikeKey(bizType, bizId);
            long ts = System.currentTimeMillis();
            RScript script = redissonClient.getScript();
            Long result = script.eval(
                    RScript.Mode.READ_WRITE,
                    LIKE_LUA_SCRIPT,
                    RScript.ReturnType.INTEGER,
                    Collections.singletonList(likeKey),
                    userId.toString(),
                    String.valueOf(ts)
            );
            log.debug("点赞操作完成: bizType={}, bizId={}, userId={}, result={}", 
                    bizType, bizId, userId, result);
            return result;
        } catch (Exception e) {
            log.error("点赞操作异常: bizType={}, bizId={}, userId={}, error={}", 
                    bizType, bizId, userId, e.getMessage());
            return null;
        }
    }

    /**
     * 获取点赞数量
     * 
     * @param bizType 业务类型
     * @param bizId 业务ID
     * @return 点赞数量
     */
    public long getLikeCount(Integer bizType, Long bizId) {
        try {
            String likeKey = RedisConfig.buildLikeKey(bizType, bizId);
            RScoredSortedSet<String> sortedSet = redissonClient.getScoredSortedSet(likeKey);
            return sortedSet.size();
        } catch (Exception e) {
            log.error("获取点赞数量异常: bizType={}, bizId={}, error={}", bizType, bizId, e.getMessage());
            return 0;
        }
    }

    /**
     * 检查用户是否已点赞
     * 
     * @param bizType 业务类型
     * @param bizId 业务ID
     * @param userId 用户ID
     * @return true-已点赞，false-未点赞
     */
    public boolean isLiked(Integer bizType, Long bizId, Long userId) {
        try {
            String likeKey = RedisConfig.buildLikeKey(bizType, bizId);
            RScoredSortedSet<String> sortedSet = redissonClient.getScoredSortedSet(likeKey);
            return sortedSet.contains(String.valueOf(userId));
        } catch (Exception e) {
            log.error("检查点赞状态异常: bizType={}, bizId={}, userId={}, error={}", 
                    bizType, bizId, userId, e.getMessage());
            return false;
        }
    }

    /**
     * 删除点赞数据（用于数据清理）
     * 
     * @param bizType 业务类型
     * @param bizId 业务ID
     */
    public void deleteLikeData(Integer bizType, Long bizId) {
        try {
            String likeKey = RedisConfig.buildLikeKey(bizType, bizId);
            redissonClient.getKeys().delete(likeKey);
            log.debug("删除点赞数据: bizType={}, bizId={}", bizType, bizId);
        } catch (Exception e) {
            log.error("删除点赞数据异常: bizType={}, bizId={}, error={}", bizType, bizId, e.getMessage());
        }
    }

    /**
     * 获取点赞用户列表（用于数据同步）
     * 
     * @param bizType 业务类型
     * @param bizId 业务ID
     * @return 用户ID列表
     */
    public java.util.Collection<String> getLikedUsers(Integer bizType, Long bizId) {
        try {
            String likeKey = RedisConfig.buildLikeKey(bizType, bizId);
            RScoredSortedSet<String> sortedSet = redissonClient.getScoredSortedSet(likeKey);
            return sortedSet.readAll();
        } catch (Exception e) {
            log.error("获取点赞用户列表异常: bizType={}, bizId={}, error={}", bizType, bizId, e.getMessage());
            return Collections.emptyList();
        }
    }
}
