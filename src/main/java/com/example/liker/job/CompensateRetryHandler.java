
package com.example.liker.job;

import com.example.liker.service.CompensateService;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * XXL-Job补偿重试任务处理器
 * 处理Kafka消息消费失败后的补偿重试
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CompensateRetryHandler {

    private final CompensateService compensateService;

    /**
     * 补偿重试任务
     * cron: 每30秒执行一次
     */
    @XxlJob("like_compensate_retry")
    public void execute() {
        log.info("XXL-Job补偿重试任务开始执行");
        try {
            String param = XxlJobHelper.getJobParam();
            log.debug("任务参数: {}", param);

            int[] result = compensateService.processPendingCompensate();
            
            String resultMsg = String.format("补偿重试完成: 成功=%d, 失败=%d, 人工处理=%d",
                    result[0], result[1], result[2]);
            
            log.info(resultMsg);
            XxlJobHelper.handleSuccess(resultMsg);
        } catch (Exception e) {
            log.error("补偿重试任务执行失败: {}", e.getMessage(), e);
            XxlJobHelper.handleFail(e.getMessage());
        }
    }
}
