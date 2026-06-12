
package com.example.liker.service.impl;

import com.example.liker.config.RedisConfig;
import com.example.liker.common.constants.LikeConstant;
import com.example.liker.common.exception.BusinessException;
import com.example.liker.common.util.RedisLikeUtil;
import com.example.liker.common.util.RedisRateLimiter;
import com.example.liker.dto.request.LikeCheckRequest;
import com.example.liker.dto.request.LikeCountRequest;
import com.example.liker.dto.request.LikeOperateRequest;
import com.example.liker.dto.response.LikeCheckResponse;
import com.example.liker.dto.response.LikeCountResponse;
import com.example.liker.dto.response.LikeOperateResponse;
import com.example.liker.mq.msg.LikeMqMsg;
import com.example.liker.mq.producer.KafkaProducerServiceImpl;
import com.example.liker.service.LikeService;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

/**
 * 点赞服务实现类
 * 职责：业务编排、流程控制、调用原子方法
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LikeServiceImpl implements LikeService {

    private final RedisLikeUtil redisLikeUtil;
    private final RedisRateLimiter redisRateLimiter;
    private final KafkaProducerServiceImpl kafkaProducerService;

    @Qualifier("likeCountCache")
    private final Cache<String, Long> likeCountCache;

    @Qualifier("emptyValueCache")
    private final Cache<String, Boolean> emptyValueCache;

    @Value("${liker.rate-limit.user-limit-count:5}")
    private int userLimitCount;

    @Value("${liker.rate-limit.user-limit-seconds:1}")
    private int userLimitSeconds;

    @Override
    public LikeOperateResponse doLike(LikeOperateRequest request) {
        log.debug("开始处理点赞请求: {}", request);

        validateOperateRequest(request);
        checkRateLimit(request.getUserId());

        Long operateResult = executeLikeOperation(request);
        long likeCount = getLikeCountFromRedis(request.getBizType(), request.getBizId());

        invalidateLocalCache(request.getBizType(), request.getBizId());
        sendLikeMessage(request, operateResult);

        return buildOperateResponse(operateResult, likeCount);
    }

    @Override
    public LikeCountResponse getLikeCount(LikeCountRequest request) {
        log.debug("开始处理获取点赞数量请求: {}", request);

        validateCountRequest(request);
        long count = getLikeCountWithCache(request.getBizType(), request.getBizId());

        return LikeCountResponse.builder()
                .success(true)
                .likeCount(count)
                .build();
    }

    @Override
    public LikeCheckResponse checkUserLiked(LikeCheckRequest request) {
        log.debug("开始处理检查点赞状态请求: {}", request);

        validateCheckRequest(request);
        boolean liked = checkUserLikedStatus(request.getBizType(), request.getBizId(), request.getUserId());

        return LikeCheckResponse.builder()
                .success(true)
                .liked(liked)
                .build();
    }

    private void validateOperateRequest(LikeOperateRequest request) {
        if (ObjectUtils.isEmpty(request.getBizType())) {
            throw BusinessException.invalidParam("bizType不能为空");
        }
        if (ObjectUtils.isEmpty(request.getBizId())) {
            throw BusinessException.invalidParam("bizId不能为空");
        }
        if (ObjectUtils.isEmpty(request.getUserId())) {
            throw BusinessException.invalidParam("userId不能为空");
        }
    }

    private void validateCountRequest(LikeCountRequest request) {
        if (ObjectUtils.isEmpty(request.getBizType())) {
            throw BusinessException.invalidParam("bizType不能为空");
        }
        if (ObjectUtils.isEmpty(request.getBizId())) {
            throw BusinessException.invalidParam("bizId不能为空");
        }
    }

    private void validateCheckRequest(LikeCheckRequest request) {
        if (ObjectUtils.isEmpty(request.getBizType())) {
            throw BusinessException.invalidParam("bizType不能为空");
        }
        if (ObjectUtils.isEmpty(request.getBizId())) {
            throw BusinessException.invalidParam("bizId不能为空");
        }
        if (ObjectUtils.isEmpty(request.getUserId())) {
            throw BusinessException.invalidParam("userId不能为空");
        }
    }

    private void checkRateLimit(Long userId) {
        if (!redisRateLimiter.tryAcquire(userId, userLimitCount, userLimitSeconds)) {
            throw BusinessException.rateLimit();
        }
    }

    private Long executeLikeOperation(LikeOperateRequest request) {
        Long result = redisLikeUtil.like(request.getBizType(), request.getBizId(), request.getUserId());
        
        if (ObjectUtils.isEmpty(result)) {
            log.error("点赞操作失败: {}", request);
            throw BusinessException.serviceError("操作失败");
        }
        
        return result;
    }

    private long getLikeCountFromRedis(Integer bizType, Long bizId) {
        return redisLikeUtil.getLikeCount(bizType, bizId);
    }

    private long getLikeCountWithCache(Integer bizType, Long bizId) {
        String cacheKey = buildCacheKey(bizType, bizId);
        String emptyKey = RedisConfig.buildEmptyValueKey(bizType, bizId);

        Long count = likeCountCache.getIfPresent(cacheKey);
        if (!ObjectUtils.isEmpty(count)) {
            log.debug("从本地缓存获取点赞数: {}", count);
            return count;
        }

        if (!ObjectUtils.isEmpty(emptyValueCache.getIfPresent(emptyKey))) {
            return 0;
        }

        long redisCount = redisLikeUtil.getLikeCount(bizType, bizId);
        
        if (redisCount > 0) {
            likeCountCache.put(cacheKey, redisCount);
        } else {
            emptyValueCache.put(emptyKey, true);
        }

        return redisCount;
    }

    private boolean checkUserLikedStatus(Integer bizType, Long bizId, Long userId) {
        return redisLikeUtil.isLiked(bizType, bizId, userId);
    }

    private void invalidateLocalCache(Integer bizType, Long bizId) {
        String cacheKey = buildCacheKey(bizType, bizId);
        likeCountCache.invalidate(cacheKey);
        emptyValueCache.invalidate(RedisConfig.buildEmptyValueKey(bizType, bizId));
        log.debug("本地缓存已失效: {}", cacheKey);
    }

    private void sendLikeMessage(LikeOperateRequest request, Long operateResult) {
        try {
            LikeMqMsg mqMsg = LikeMqMsg.builder()
                    .bizType(request.getBizType())
                    .bizId(request.getBizId())
                    .userId(request.getUserId())
                    .operateType(operateResult == LikeConstant.OP_RESULT_LIKE 
                            ? LikeConstant.OPERATE_TYPE_LIKE 
                            : LikeConstant.OPERATE_TYPE_CANCEL)
                    .operateTs(System.currentTimeMillis())
                    .build();

            kafkaProducerService.sendMsg(mqMsg);
            log.debug("Kafka消息发送成功: {}", mqMsg);
        } catch (Exception e) {
            log.error("Kafka消息发送失败: {}", e.getMessage());
        }
    }

    private LikeOperateResponse buildOperateResponse(Long operateResult, long likeCount) {
        boolean liked = operateResult == LikeConstant.OP_RESULT_LIKE;
        
        log.info("点赞操作完成: liked={}, likeCount={}", liked, likeCount);
        
        return LikeOperateResponse.builder()
                .success(true)
                .liked(liked)
                .likeCount(likeCount)
                .build();
    }

    private String buildCacheKey(Integer bizType, Long bizId) {
        return bizType + ":" + bizId;
    }
}
