
package com.example.liker.controller;

import com.example.liker.dto.request.LikeCheckRequest;
import com.example.liker.dto.request.LikeCountRequest;
import com.example.liker.dto.request.LikeOperateRequest;
import com.example.liker.dto.response.ApiResponse;
import com.example.liker.dto.response.LikeCheckResponse;
import com.example.liker.dto.response.LikeCountResponse;
import com.example.liker.dto.response.LikeOperateResponse;
import com.example.liker.service.LikeService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 点赞控制器
 * 职责：接收请求、参数校验、调用Service、返回响应
 */
@Slf4j
@RestController
@RequestMapping("/api/like")
@RequiredArgsConstructor
public class LikeController {

    private final LikeService likeService;

    /**
     * 点赞/取消点赞接口
     */
    @PostMapping("/operate")
    public ResponseEntity<ApiResponse<LikeOperateResponse>> doLike(@Valid @RequestBody LikeOperateRequest request) {
        log.info("收到点赞请求: {}", request);
        LikeOperateResponse response = likeService.doLike(request);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 获取点赞数量接口
     */
    @PostMapping("/count")
    public ResponseEntity<ApiResponse<LikeCountResponse>> getLikeCount(@Valid @RequestBody LikeCountRequest request) {
        log.info("收到获取点赞数量请求: {}", request);
        LikeCountResponse response = likeService.getLikeCount(request);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    /**
     * 检查用户点赞状态接口
     */
    @PostMapping("/check")
    public ResponseEntity<ApiResponse<LikeCheckResponse>> checkUserLiked(@Valid @RequestBody LikeCheckRequest request) {
        log.info("收到检查点赞状态请求: {}", request);
        LikeCheckResponse response = likeService.checkUserLiked(request);
        return ResponseEntity.ok(ApiResponse.success(response));
    }
}
