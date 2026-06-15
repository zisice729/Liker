package com.example.liker.job;

import com.example.liker.constant.CommonConst;
import com.example.liker.entity.LikeCompensate;
import com.example.liker.entity.LikesRecord;
import com.example.liker.mapper.LikeCompensateMapper;
import com.example.liker.mapper.LikesRecordMapper;
import com.example.liker.mq.dto.LikeKafkaMsg;
import com.example.liker.util.AlertUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * XXL-Job 补偿重试任务
 * 负责重试消费失败的Kafka消息，保证数据最终一致性
 * 
 * 调度配置：
 * - Job Handler: likeCompensateJobHandler
 * - Cron: 0/1 * * * * ? (每分钟执行)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CompensateJob {

    private final LikeCompensateMapper compensateMapper;
    private final LikesRecordMapper recordMapper;
    private final RedissonClient redissonClient;
    private final AlertUtil alertUtil;
    private final ObjectMapper objectMapper;

    /**
     * 执行补偿重试任务
     * 1. 获取分布式锁防止集群重复执行
     * 2. 查询待重试的补偿消息
     * 3. 逐条重试，成功则更新状态为完成
     * 4. 重试次数耗尽则标记为人工处理并告警
     */
    @XxlJob("likeCompensateJobHandler")
    public void execute() throws Exception {
        RLock lock = redissonClient.getLock(CommonConst.JOB_LOCK_COMPENSATE);
        try {
            // 获取分布式锁，最多等待0秒，持有70秒
            if (!lock.tryLock(0, 70, TimeUnit.SECONDS)) {
                XxlJobHelper.log("获取分布式锁失败，跳过本次执行");
                return;
            }
            
            // 查询待重试数据
            List<LikeCompensate> waitList = compensateMapper.selectWaitRetryData(CommonConst.MAX_RETRY);
            if (waitList.isEmpty()) {
                XxlJobHelper.log("没有待重试数据");
                return;
            }
            
            // 逐条处理
            for (LikeCompensate item : waitList) {
                LikeKafkaMsg msg = objectMapper.readValue(item.getMsgBody(), LikeKafkaMsg.class);
                try {
                    // 重试写入MySQL
                    LikesRecord po = new LikesRecord();
                    po.setObjId(msg.getObjId());
                    po.setUserId(msg.getUserId());
                    po.setStatus(msg.getAction());
                    recordMapper.insertOrUpdate(po);
                    item.setStatus(2); // 标记为成功
                } catch (Exception e) {
                    // 重试失败，增加重试次数
                    item.setRetryTimes(item.getRetryTimes() + 1);
                    item.setErrorMsg(e.getMessage());
                    // 重试次数耗尽，标记为人工处理并告警
                    if (item.getRetryTimes() >= CommonConst.MAX_RETRY) {
                        item.setStatus(3);
                        alertUtil.sendAlert("点赞消息重试次数用尽，请人工处理");
                    }
                }
                // 更新补偿记录状态
                compensateMapper.updateById(item);
            }
            
            XxlJobHelper.handleSuccess("补偿重试完成，处理数量: " + waitList.size());
        } finally {
            // 释放锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}