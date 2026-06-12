
package com.example.liker.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

/**
 * 检查点赞状态请求DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LikeCheckRequest {

    /**
     * 业务类型：1-文章 2-评论
     */
    @NotNull(message = "bizType不能为空")
    private Integer bizType;

    /**
     * 业务ID
     */
    @NotNull(message = "bizId不能为空")
    private Long bizId;

    /**
     * 用户ID
     */
    @NotNull(message = "userId不能为空")
    private Long userId;
}
