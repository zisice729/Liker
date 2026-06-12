
package com.example.liker.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 点赞操作响应DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LikeOperateResponse {

    /**
     * 是否成功
     */
    private Boolean success;

    /**
     * 是否已点赞
     */
    private Boolean liked;

    /**
     * 点赞数量
     */
    private Long likeCount;

    /**
     * 错误消息
     */
    private String message;
}
