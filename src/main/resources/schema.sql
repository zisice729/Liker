
-- 点赞归档表（分库分表，需根据实际分片规则创建）
-- 这里仅展示单表示例，实际部署时需要创建 ds0.user_like_0~127 和 ds1.user_like_0~127

CREATE TABLE IF NOT EXISTS user_like_0 (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    biz_type TINYINT NOT NULL COMMENT '1文章 2评论',
    biz_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    operate_type TINYINT NOT NULL COMMENT '1点赞 2取消点赞',
    operate_time DATETIME NOT NULL,
    UNIQUE KEY uk_biztype_bizid_uid (biz_type, biz_id, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='点赞归档表';

-- Kafka消费补偿表（不分库分表）
CREATE TABLE IF NOT EXISTS like_msg_compensate (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    msg_unique_key VARCHAR(150) NOT NULL COMMENT '消息全局唯一标识',
    msg_body JSON NOT NULL COMMENT '原始MQ消息',
    retry_count INT DEFAULT 0 COMMENT '已重试次数',
    max_retry INT DEFAULT 8 COMMENT '最大自动重试次数',
    status TINYINT DEFAULT 1 COMMENT '1待重试 2成功 3人工处理',
    error_msg VARCHAR(1000),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status_retry(status, retry_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Kafka消费补偿表';
