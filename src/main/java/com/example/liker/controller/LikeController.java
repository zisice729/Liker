package com.example.liker.controller;

import com.example.liker.service.LikeService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 点赞控制器
 * 提供点赞相关的REST API接口
 */
@RestController
@RequestMapping("/api/like")
@RequiredArgsConstructor
public class LikeController {

    private final LikeService likeService;

    /**
     * 执行点赞/取消点赞操作
     *
     * @param objId 业务对象ID（文章/评论）
     * @param userId 用户ID
     * @return 最新点赞数
     */
    @PostMapping("/operate")
    public ResponseEntity<Long> operate(@RequestParam Long objId, @RequestParam Long userId) {
        return ResponseEntity.ok(likeService.operateLike(objId, userId));
    }

    /**
     * 获取指定对象的点赞数
     *
     * @param objId 业务对象ID
     * @return 点赞数量
     */
    @GetMapping("/count")
    public ResponseEntity<Long> getCount(@RequestParam Long objId) {
        return ResponseEntity.ok(likeService.getLikeCount(objId));
    }

    /**
     * 检查用户是否已点赞指定对象
     *
     * @param objId 业务对象ID
     * @param userId 用户ID
     * @return true-已点赞，false-未点赞
     */
    @GetMapping("/check")
    public ResponseEntity<Boolean> check(@RequestParam Long objId, @RequestParam Long userId) {
        return ResponseEntity.ok(likeService.checkUserLiked(objId, userId));
    }
}