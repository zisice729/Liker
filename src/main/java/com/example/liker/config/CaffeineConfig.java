
package com.example.liker.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Caffeine本地缓存配置类
 */
@Configuration
public class CaffeineConfig {

    @Value("${liker.cache.caffeine.max-size:100000}")
    private int maxSize;

    @Value("${liker.cache.caffeine.expire-after-write-ms:100}")
    private int expireAfterWriteMs;

    /**
     * 点赞数量缓存
     */
    @Bean("likeCountCache")
    public Cache<String, Long> likeCountCache() {
        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireAfterWriteMs, TimeUnit.MILLISECONDS)
                .recordStats()
                .build();
    }

    /**
     * 空值缓存（防穿透）
     */
    @Bean("emptyValueCache")
    public Cache<String, Boolean> emptyValueCache(@Value("${liker.cache.caffeine.empty-value-expire-minutes:5}") int expireMinutes) {
        return Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .build();
    }
}
