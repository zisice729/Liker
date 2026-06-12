
package com.example.liker.common.util;

import com.example.liker.common.constants.LikeConstant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

/**
 * Redis限流工具类
 * 基于Redisson的RRateLimiter实现，提供分布式限流能力
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisRateLimiter {

    private final RedissonClient redissonClient;

    /**
     * 尝试获取令牌（限流检查）
     * 
     * @param userId 用户ID
     * @param permits 允许的请求数量
     * @param seconds 时间窗口（秒）
     * @return true-获取成功，false-限流
     */
    public boolean tryAcquire(Long userId, int permits, int seconds) {
        try {
            String key = LikeConstant.REDIS_KEY_LIMIT_PREFIX + userId;
            RRateLimiter rateLimiter = redissonClient.getRateLimiter(key);
            
            if (!rateLimiter.isExists()) {
                rateLimiter.trySetRate(RateType.OVERALL, permits, seconds, RateIntervalUnit.SECONDS);
            }
            
            boolean acquired = rateLimiter.tryAcquire();
            if (!acquired) {
                log.warn("用户被限流: userId={}", userId);
            }
            return acquired;
        } catch (Exception e) {
            log.error("限流检查异常: userId={}, error={}", userId, e.getMessage());
            return true;
        }
    }

    /**
     * 尝试获取指定数量的令牌
     * 
     * @param userId 用户ID
     * @param permits 允许的请求数量
     * @param seconds 时间窗口（秒）
     * @param acquireCount 要获取的令牌数量
     * @return true-获取成功，false-限流
     */
    public boolean tryAcquire(Long userId, int permits, int seconds, int acquireCount) {
        try {
            String key = LikeConstant.REDIS_KEY_LIMIT_PREFIX + userId;
            RRateLimiter rateLimiter = redissonClient.getRateLimiter(key);
            
            if (!rateLimiter.isExists()) {
                rateLimiter.trySetRate(RateType.OVERALL, permits, seconds, RateIntervalUnit.SECONDS);
            }
            
            boolean acquired = rateLimiter.tryAcquire(acquireCount);
            if (!acquired) {
                log.warn("用户被限流: userId={}, acquireCount={}", userId, acquireCount);
            }
            return acquired;
        } catch (Exception e) {
            log.error("限流检查异常: userId={}, error={}", userId, e.getMessage());
            return true;
        }
    }

    /**
     * 释放限流资源（用于测试或特殊场景）
     * 
     * @param userId 用户ID
     */
    public void release(Long userId) {
        try {
            String key = LikeConstant.REDIS_KEY_LIMIT_PREFIX + userId;
            redissonClient.getKeys().delete(key);
        } catch (Exception e) {
            log.error("释放限流资源异常: userId={}, error={}", userId, e.getMessage());
        }
    }
}
