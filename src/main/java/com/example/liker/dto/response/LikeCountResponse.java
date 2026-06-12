
package com.example.liker.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 获取点赞数量响应DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LikeCountResponse {

    /**
     * 是否成功
     */
    private Boolean success;

    /**
     * 点赞数量
     */
    private Long likeCount;

    /**
     * 错误消息
     */
    private String message;
}
