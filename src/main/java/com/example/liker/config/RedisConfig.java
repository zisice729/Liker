
package com.example.liker.config;

import com.example.liker.constant.LikeConstant;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Redis配置类
 */
@Configuration
public class RedisConfig {

    /**
     * 点赞操作的Lua脚本
     * 返回值：1=点赞成功，0=取消点赞成功
     */
    public static final String LIKE_LUA_SCRIPT = 
        "local zKey = KEYS[1]\n" +
        "local userId = ARGV[1]\n" +
        "local ts = tonumber(ARGV[2])\n" +
        "local score = redis.call('ZSCORE', zKey, userId)\n" +
        "if score ~= nil then\n" +
        "    redis.call('ZREM', zKey, userId)\n" +
        "    return 0\n" +
        "else\n" +
        "    redis.call('ZADD', zKey, ts, userId)\n" +
        "    return 1\n" +
        "end";

    /**
     * 构建点赞Key
     */
    public static String buildLikeKey(Integer bizType, Long bizId) {
        return LikeConstant.REDIS_KEY_LIKE_PREFIX + bizType + ":" + bizId;
    }

    /**
     * 构建用户限流Key
     */
    public static String buildLimitKey(Long userId) {
        return LikeConstant.REDIS_KEY_LIMIT_PREFIX + userId;
    }

    /**
     * 构建空值缓存Key
     */
    public static String buildEmptyValueKey(Integer bizType, Long bizId) {
        return LikeConstant.REDIS_KEY_EMPTY_PREFIX + bizType + ":" + bizId;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(connectionFactory);
        return template;
    }
}
