package com.example.liker.config;

import com.example.liker.constant.CommonConst;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Redis配置类
 * 包含：RedisTemplate配置、Lua脚本配置、Pub/Sub缓存失效通知配置
 */
@Configuration
public class RedisConfig {

    /**
     * 配置RedisTemplate
     * 配置String和JSON序列化器
     *
     * @param factory Redis连接工厂
     * @return RedisTemplate实例
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    /**
     * 配置点赞操作Lua脚本
     * 用于实现原子化的点赞/取消点赞操作
     *
     * @return DefaultRedisScript实例
     */
    @Bean
    public DefaultRedisScript<Long> likeOperateScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new org.springframework.scripting.support.ResourceScriptSource(
                new ClassPathResource("lua/like_operate.lua")));
        script.setResultType(Long.class);
        return script;
    }

    /**
     * 配置Redis消息监听容器
     * 用于监听缓存失效通知通道
     *
     * @param factory Redis连接工厂
     * @param listenerAdapter 消息监听适配器
     * @return RedisMessageListenerContainer实例
     */
    @Bean
    public RedisMessageListenerContainer container(RedisConnectionFactory factory,
                                                   MessageListenerAdapter listenerAdapter) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        container.addMessageListener(listenerAdapter, 
                new ChannelTopic(CommonConst.CACHE_INVALID_CHANNEL));
        return container;
    }

    /**
     * 配置消息监听适配器
     *
     * @param listener 缓存失效监听器
     * @return MessageListenerAdapter实例
     */
    @Bean
    public MessageListenerAdapter listenerAdapter(CacheInvalidListener listener) {
        return new MessageListenerAdapter(listener, "onMessage");
    }

    /**
     * 缓存失效监听器内部类
     * 监听Redis Pub/Sub消息，收到通知后失效本地Caffeine缓存
     */
    @org.springframework.stereotype.Component
    public static class CacheInvalidListener {

        private final LoadingCache<String, Long> likeCountCache;

        public CacheInvalidListener(LoadingCache<String, Long> likeCountCache) {
            this.likeCountCache = likeCountCache;
        }

        /**
         * 处理缓存失效消息
         * @param cacheKey 需要失效的缓存Key
         */
        public void onMessage(String cacheKey) {
            likeCountCache.invalidate(cacheKey);
        }
    }
}