package com.example.liker.constant;

/**
 * 通用常量类
 * 定义系统全局使用的常量配置
 */
public class CommonConst {

    /**
     * Set分片总数，用于将用户点赞数据分散存储，避免单Key过大
     */
    public static final int SHARD_COUNT = 100;

    /**
     * 消息最大自动重试次数，超过后标记为人工处理
     */
    public static final int MAX_RETRY = 3;

    /**
     * Caffeine本地缓存过期时间（秒）
     */
    public static final int CAFFEINE_EXPIRE = 5;

    /**
     * 冷数据判定天数，超过该天数的数据可归档
     */
    public static final int COLD_DATA_DAY = 30;

    /**
     * Redis Pub/Sub 本地缓存失效通知通道
     */
    public static final String CACHE_INVALID_CHANNEL = "like_cache_invalid_channel";

    /**
     * XXL-Job 补偿重试任务分布式锁Key
     */
    public static final String JOB_LOCK_COMPENSATE = "like:job:compensate:lock";

    /**
     * XXL-Job 数据对账任务分布式锁Key
     */
    public static final String JOB_LOCK_CHECK = "like:job:check:lock";
}