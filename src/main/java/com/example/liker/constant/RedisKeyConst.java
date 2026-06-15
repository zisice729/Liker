package com.example.liker.constant;

/**
 * Redis Key 常量类
 * 定义Redis中使用的Key模板
 */
public class RedisKeyConst {

    /**
     * 点赞总数Key模板
     * 格式: like:cnt:{objId}
     * 存储类型: String
     */
    public static final String LIKE_COUNT_KEY = "like:cnt:%s";

    /**
     * 点赞用户集合Key模板（分片存储）
     * 格式: like:set:{objId}:{shardIndex}
     * 存储类型: Set
     */
    public static final String LIKE_SET_KEY = "like:set:%s:%s";
}