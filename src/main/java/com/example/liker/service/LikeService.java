package com.example.liker.service;

/**
 * 点赞服务接口
 * 定义点赞相关的业务操作
 */
public interface LikeService {

    /**
     * 执行点赞/取消点赞操作
     *
     * @param objId 业务对象ID（文章/评论）
     * @param userId 用户ID
     * @return 最新点赞数
     */
    Long operateLike(Long objId, Long userId);

    /**
     * 获取指定对象的点赞数
     *
     * @param objId 业务对象ID
     * @return 点赞数量
     */
    Long getLikeCount(Long objId);

    /**
     * 检查用户是否已点赞指定对象
     *
     * @param objId 业务对象ID
     * @param userId 用户ID
     * @return true-已点赞，false-未点赞
     */
    Boolean checkUserLiked(Long objId, Long userId);
}