
package com.example.liker.common.constants;

/**
 * 点赞系统常量类
 */
public class LikeConstant {

    private LikeConstant() {
        // 私有构造函数，防止实例化
    }

    // ==================== 业务类型 ====================
    public static final int BIZ_TYPE_POST = 1;      // 帖子
    public static final int BIZ_TYPE_COMMENT = 2;   // 评论
    public static final int BIZ_TYPE_REPLY = 3;     // 回复

    // ==================== 操作类型 ====================
    public static final int OPERATE_TYPE_LIKE = 1;      // 点赞
    public static final int OPERATE_TYPE_CANCEL = 2;    // 取消点赞

    // ==================== 操作结果 ====================
    public static final int OP_RESULT_LIKE = 1;     // 点赞成功
    public static final int OP_RESULT_CANCEL = 0;   // 取消点赞成功

    // ==================== Redis Key前缀 ====================
    public static final String REDIS_KEY_LIKE_PREFIX = "like:";      // 点赞Key前缀
    public static final String REDIS_KEY_LIMIT_PREFIX = "limit:";    // 限流Key前缀
    public static final String REDIS_KEY_EMPTY_PREFIX = "empty:";    // 空值缓存Key前缀

    // ==================== Kafka配置 ====================
    public static final String KAFKA_TOPIC_LIKE = "topic_like";      // Kafka主题

    // ==================== 分布式锁Key ====================
    public static final String LOCK_KEY_COMPENSATE = "lock:compensate";   // 补偿任务锁
    public static final String LOCK_KEY_RECONCILE = "lock:reconcile";     // 对账任务锁

    // ==================== 补偿状态 ====================
    public static final int COMPENSATE_STATUS_PENDING = 1;   // 待补偿
    public static final int COMPENSATE_STATUS_SUCCESS = 2;   // 补偿成功
    public static final int COMPENSATE_STATUS_MANUAL = 3;    // 需人工处理
}
