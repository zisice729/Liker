
package com.example.liker.common.lock;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Redis分布式锁模板类
 * 基于Redisson封装，提供统一的分布式锁操作
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisLockTemplate {

    private final RedissonClient redissonClient;

    /**
     * 带超时时间的锁模板方法
     * 
     * @param lockKey 锁键
     * @param waitTime 等待时间（秒）
     * @param leaseTime 持有时间（秒）
     * @param supplier 业务逻辑
     * @return 业务逻辑返回值，获取锁失败返回null
     */
    public <T> T executeWithLock(String lockKey, long waitTime, long leaseTime, 
                                  java.util.function.Supplier<T> supplier) {
        RLock lock = redissonClient.getLock(lockKey);
        
        try {
            boolean locked = lock.tryLock(waitTime, leaseTime, TimeUnit.SECONDS);
            if (!locked) {
                log.debug("获取锁失败: {}", lockKey);
                return null;
            }
            
            try {
                return supplier.get();
            } finally {
                if (lock.isLocked() && lock.isHeldByCurrentThread()) {
                    lock.unlock();
                    log.debug("释放锁: {}", lockKey);
                }
            }
        } catch (InterruptedException e) {
            log.error("获取锁被中断: {}", lockKey);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * 无返回值的锁模板方法
     * 
     * @param lockKey 锁键
     * @param waitTime 等待时间（秒）
     * @param leaseTime 持有时间（秒）
     * @param runnable 业务逻辑
     * @return true-执行成功，false-获取锁失败
     */
    public boolean executeWithLock(String lockKey, long waitTime, long leaseTime, 
                                   Runnable runnable) {
        RLock lock = redissonClient.getLock(lockKey);
        
        try {
            boolean locked = lock.tryLock(waitTime, leaseTime, TimeUnit.SECONDS);
            if (!locked) {
                log.debug("获取锁失败: {}", lockKey);
                return false;
            }
            
            try {
                runnable.run();
                return true;
            } finally {
                if (lock.isLocked() && lock.isHeldByCurrentThread()) {
                    lock.unlock();
                    log.debug("释放锁: {}", lockKey);
                }
            }
        } catch (InterruptedException e) {
            log.error("获取锁被中断: {}", lockKey);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 获取锁对象（用于复杂场景）
     * 
     * @param lockKey 锁键
     * @return RLock对象
     */
    public RLock getLock(String lockKey) {
        return redissonClient.getLock(lockKey);
    }

    /**
     * 尝试获取锁
     * 
     * @param lockKey 锁键
     * @param waitTime 等待时间（秒）
     * @param leaseTime 持有时间（秒）
     * @return true-获取成功，false-获取失败
     */
    public boolean tryLock(String lockKey, long waitTime, long leaseTime) {
        RLock lock = redissonClient.getLock(lockKey);
        try {
            return lock.tryLock(waitTime, leaseTime, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 释放锁
     * 
     * @param lockKey 锁键
     */
    public void unlock(String lockKey) {
        RLock lock = redissonClient.getLock(lockKey);
        if (lock.isLocked() && lock.isHeldByCurrentThread()) {
            lock.unlock();
            log.debug("释放锁: {}", lockKey);
        }
    }
}
