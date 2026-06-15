package com.example.liker.config;

import com.example.liker.constant.CommonConst;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.TimeUnit;

/**
 * Caffeine本地缓存配置类
 * 配置用于缓存点赞数的本地缓存实例
 */
@Configuration
public class CaffeineConfig {

    /**
     * 配置点赞数本地缓存
     * - 写入后5秒过期
     * - 最大容量100000条
     * - 自动从Redis加载数据
     *
     * @param redisTemplate Redis操作模板
     * @return LoadingCache实例
     */
    @Bean
    public LoadingCache<String, Long> likeCountCache(RedisTemplate<String, Object> redisTemplate) {
        return Caffeine.newBuilder()
                .expireAfterWrite(CommonConst.CAFFEINE_EXPIRE, TimeUnit.SECONDS)
                .maximumSize(100000)
                .build(key -> {
                    Object val = redisTemplate.opsForValue().get(key);
                    return val == null ? 0L : Long.parseLong(val.toString());
                });
    }
}