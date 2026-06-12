
package com.example.liker.job;

import com.example.liker.service.ReconcileService;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * XXL-Job对账任务处理器
 * 定期核对Redis与MySQL数据一致性
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReconcileHandler {

    private final ReconcileService reconcileService;

    /**
     * 对账任务
     * cron: 每小时执行一次
     */
    @XxlJob("like_reconcile")
    public void execute() {
        log.info("XXL-Job对账任务开始执行");
        try {
            String param = XxlJobHelper.getJobParam();
            log.debug("任务参数: {}", param);

            int[] result = reconcileService.doReconcile();
            
            String resultMsg = String.format("对账完成: 检查数据=%d条, 不一致=%d条",
                    result[0], result[1]);
            
            log.info(resultMsg);
            XxlJobHelper.handleSuccess(resultMsg);
        } catch (Exception e) {
            log.error("对账任务执行失败: {}", e.getMessage(), e);
            XxlJobHelper.handleFail(e.getMessage());
        }
    }
}
