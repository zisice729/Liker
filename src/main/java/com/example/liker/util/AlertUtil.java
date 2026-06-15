package com.example.liker.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 告警工具类
 * 用于发送系统告警通知，可对接钉钉、企业微信等告警渠道
 */
@Slf4j
@Component
public class AlertUtil {

    /**
     * 发送告警通知
     * @param content 告警内容
     */
    public void sendAlert(String content) {
        // TODO: 对接钉钉/企业微信等告警通道
        log.error("【告警通知】{}", content);
    }
}