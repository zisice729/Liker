
package com.example.liker.common.exception;

import lombok.Getter;

/**
 * 业务异常类
 */
@Getter
public class BusinessException extends RuntimeException {

    private final String code;
    private final String message;

    public BusinessException(String code, String message) {
        super(message);
        this.code = code;
        this.message = message;
    }

    /**
     * 参数无效
     */
    public static BusinessException invalidParam(String message) {
        return new BusinessException("PARAM_ERROR", message);
    }

    /**
     * 限流
     */
    public static BusinessException rateLimit() {
        return new BusinessException("RATE_LIMIT", "操作过于频繁，请稍后再试");
    }

    /**
     * 服务错误
     */
    public static BusinessException serviceError(String message) {
        return new BusinessException("SERVICE_ERROR", message);
    }

    /**
     * 资源不存在
     */
    public static BusinessException notFound(String message) {
        return new BusinessException("NOT_FOUND", message);
    }

    /**
     * 操作失败
     */
    public static BusinessException fail(String message) {
        return new BusinessException("FAIL", message);
    }
}
