package com.example.liker.service.impl;

import com.example.liker.constant.CommonConst;
import com.example.liker.constant.RedisKeyConst;
import com.example.liker.entity.LikesRecord;
import com.example.liker.mapper.LikesRecordMapper;
import com.example.liker.mq.dto.LikeKafkaMsg;
import com.example.liker.mq.producer.LikeProducer;
import com.example.liker.service.LikeService;
import com.example.liker.util.ShardUtil;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Arrays;

/**
 * 点赞服务实现类
 * 核心业务逻辑：
 * 1. 使用Redis作为主存储，保证高并发读写
 * 2. 使用Lua脚本实现原子化点赞/取消操作
 * 3. Caffeine本地缓存+Redis Pub/Sub实现集群缓存一致性
 * 4. Kafka异步消息实现最终一致性
 * 5. Redis故障时降级到MySQL
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LikeServiceImpl implements LikeService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final DefaultRedisScript<Long> likeOperateScript;
    private final LoadingCache<String, Long> likeCountCache;
    private final ShardUtil shardUtil;
    private final LikeProducer likeProducer;
    private final LikesRecordMapper recordMapper;

    /**
     * 执行点赞/取消点赞操作
     * 流程：计算分片 -> 执行Lua脚本 -> 失效缓存 -> 发送Kafka消息
     *
     * @param objId 业务对象ID（文章/评论）
     * @param userId 用户ID
     * @return 最新点赞数
     */
    @Override
    public Long operateLike(Long objId, Long userId) {
        // 1. 计算用户分片索引
        int shardIdx = shardUtil.getShardIndex(userId);
        
        // 2. 构建Redis Key
        String countKey = String.format(RedisKeyConst.LIKE_COUNT_KEY, objId);
        String setKey = String.format(RedisKeyConst.LIKE_SET_KEY, objId, shardIdx);

        // 3. 执行Lua脚本（原子操作）
        Long newCount = redisTemplate.execute(
                likeOperateScript,
                Arrays.asList(countKey, setKey),
                userId.toString()
        );

        // 4. 失效本地缓存并广播通知集群
        likeCountCache.invalidate(countKey);
        redisTemplate.convertAndSend(CommonConst.CACHE_INVALID_CHANNEL, countKey);

        // 5. 发送Kafka消息（异步持久化到MySQL）
        LikeKafkaMsg msg = new LikeKafkaMsg();
        msg.setObjId(objId);
        msg.setUserId(userId);
        msg.setAction(newCount != null && newCount > 0 ? 1 : 0);
        msg.setTimestamp(System.currentTimeMillis());
        likeProducer.sendMsg(msg);

        return newCount;
    }

    /**
     * 获取指定对象的点赞数
     * 优先从Caffeine缓存获取，失败则降级到MySQL
     *
     * @param objId 业务对象ID
     * @return 点赞数量
     */
    @Override
    public Long getLikeCount(Long objId) {
        String countKey = String.format(RedisKeyConst.LIKE_COUNT_KEY, objId);
        try {
            return likeCountCache.get(countKey);
        } catch (Exception e) {
            log.warn("获取点赞数缓存失败，降级到MySQL: {}", e.getMessage());
            return recordMapper.countLikeByObjId(objId);
        }
    }

    /**
     * 检查用户是否已点赞指定对象
     * 优先从Redis获取，失败则降级到MySQL
     *
     * @param objId 业务对象ID
     * @param userId 用户ID
     * @return true-已点赞，false-未点赞
     */
    @Override
    public Boolean checkUserLiked(Long objId, Long userId) {
        int shardIdx = shardUtil.getShardIndex(userId);
        String setKey = String.format(RedisKeyConst.LIKE_SET_KEY, objId, shardIdx);
        try {
            Boolean exist = redisTemplate.opsForSet().isMember(setKey, userId);
            return exist != null && exist;
        } catch (Exception e) {
            log.warn("检查用户点赞状态缓存失败，降级到MySQL: {}", e.getMessage());
            Integer status = recordMapper.getUserLikeStatus(objId, userId);
            return status != null && status == 1;
        }
    }
}