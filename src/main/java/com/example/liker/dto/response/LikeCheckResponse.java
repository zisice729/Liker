
package com.example.liker.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 检查点赞状态响应DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LikeCheckResponse {

    /**
     * 是否成功
     */
    private Boolean success;

    /**
     * 是否已点赞
     */
    private Boolean liked;

    /**
     * 错误消息
     */
    private String message;
}
