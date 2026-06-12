
package com.example.liker.service;

import com.example.liker.dto.request.LikeCheckRequest;
import com.example.liker.dto.request.LikeCountRequest;
import com.example.liker.dto.request.LikeOperateRequest;
import com.example.liker.dto.response.LikeCheckResponse;
import com.example.liker.dto.response.LikeCountResponse;
import com.example.liker.dto.response.LikeOperateResponse;

/**
 * 点赞服务接口
 */
public interface LikeService {

    /**
     * 执行点赞/取消点赞操作
     * @param request 请求参数
     * @return 操作结果
     */
    LikeOperateResponse doLike(LikeOperateRequest request);

    /**
     * 获取点赞数量
     * @param request 请求参数
     * @return 点赞数量
     */
    LikeCountResponse getLikeCount(LikeCountRequest request);

    /**
     * 检查用户是否已点赞
     * @param request 请求参数
     * @return 点赞状态
     */
    LikeCheckResponse checkUserLiked(LikeCheckRequest request);
}
