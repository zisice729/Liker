
package com.example.liker.service.impl;

import com.example.liker.common.constants.LikeConstant;
import com.example.liker.common.lock.RedisLockTemplate;
import com.example.liker.mq.msg.LikeMqMsg;
import com.example.liker.repository.do.UserLikeDO;
import com.example.liker.repository.entity.LikeMsgCompensate;
import com.example.liker.repository.mapper.LikeMsgCompensateMapper;
import com.example.liker.repository.mapper.UserLikeMapper;
import com.example.liker.service.CompensateService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class CompensateServiceImpl implements CompensateService {

    private final LikeMsgCompensateMapper compensateMapper;
    private final UserLikeMapper userLikeMapper;
    private final RedisLockTemplate redisLockTemplate;
    private final ObjectMapper objectMapper;

    @Value("${liker.task.compensate.batch-size:300}")
    private int batchSize;

    @Value("${liker.task.compensate.max-retry:8}")
    private int maxRetry;

    @Override
    public int[] processPendingCompensate() {
        log.debug("开始执行补偿重试任务");

        Integer[] result = redisLockTemplate.executeWithLock(
                LikeConstant.LOCK_KEY_COMPENSATE,
                0, 35,
                this::doProcessPendingCompensate
        );

        if (result == null) {
            log.debug("补偿重试任务已在其他节点执行");
            return new int[]{0, 0, 0};
        }

        return new int[]{result[0], result[1], result[2]};
    }

    private Integer[] doProcessPendingCompensate() {
        List<LikeMsgCompensate> pendingList = queryPendingCompensate();
        if (pendingList.isEmpty()) {
            log.debug("无待重试的补偿记录");
            return new Integer[]{0, 0, 0};
        }

        ProcessResult result = processCompensateList(pendingList);
        updateCompensateStatus(result);

        log.info("补偿重试任务完成: 成功={}, 失败={}, 人工处理={}",
                result.successIds.size(), result.failIds.size(), result.manualIds.size());

        return new Integer[]{result.successIds.size(), result.failIds.size(), result.manualIds.size()};
    }

    private List<LikeMsgCompensate> queryPendingCompensate() {
        List<LikeMsgCompensate> pendingList = compensateMapper.selectPendingRetry(batchSize);
        log.info("查询到{}条待重试的补偿记录", pendingList.size());
        return pendingList;
    }

    private ProcessResult processCompensateList(List<LikeMsgCompensate> pendingList) {
        ProcessResult result = new ProcessResult();

        for (LikeMsgCompensate compensate : pendingList) {
            try {
                processSingleCompensate(compensate, result);
            } catch (Exception e) {
                log.warn("补偿重试失败: id={}, error={}", compensate.getId(), e.getMessage());
                handleProcessFailure(compensate, result);
            }
        }

        return result;
    }

    private void processSingleCompensate(LikeMsgCompensate compensate, ProcessResult result) {
        LikeMqMsg msg = objectMapper.readValue(compensate.getMsgBody(), LikeMqMsg.class);
        UserLikeDO userLike = convertToUserLike(msg);
        userLikeMapper.insertOrUpdate(userLike);
        result.successIds.add(compensate.getId());
        log.debug("补偿重试成功: id={}", compensate.getId());
    }

    private void handleProcessFailure(LikeMsgCompensate compensate, ProcessResult result) {
        int newRetryCount = compensate.getRetryCount() + 1;
        
        if (newRetryCount >= maxRetry) {
            result.manualIds.add(compensate.getId());
            sendAlert(compensate);
        } else {
            result.failIds.add(compensate.getId());
        }
    }

    private void updateCompensateStatus(ProcessResult result) {
        if (!result.successIds.isEmpty()) {
            compensateMapper.batchUpdateStatus(result.successIds, LikeConstant.COMPENSATE_STATUS_SUCCESS);
        }
        if (!result.failIds.isEmpty()) {
            compensateMapper.batchUpdateRetryCount(result.failIds, result.failIds.size(), LikeConstant.COMPENSATE_STATUS_PENDING);
        }
        if (!result.manualIds.isEmpty()) {
            compensateMapper.batchUpdateStatus(result.manualIds, LikeConstant.COMPENSATE_STATUS_MANUAL);
        }
    }

    private UserLikeDO convertToUserLike(LikeMqMsg msg) {
        return UserLikeDO.builder()
                .bizType(msg.getBizType())
                .bizId(msg.getBizId())
                .userId(msg.getUserId())
                .operateType(msg.getOperateType())
                .operateTime(LocalDateTime.ofInstant(
                        java.time.Instant.ofEpochMilli(msg.getOperateTs()),
                        ZoneId.systemDefault()))
                .build();
    }

    private void sendAlert(LikeMsgCompensate compensate) {
        try {
            log.error("补偿记录达到最大重试次数，需人工处理: id={}, msg={}",
                    compensate.getId(), compensate.getMsgBody());
        } catch (Exception e) {
            log.error("发送告警失败: {}", e.getMessage());
        }
    }

    private static class ProcessResult {
        List<Long> successIds = new ArrayList<>();
        List<Long> failIds = new ArrayList<>();
        List<Long> manualIds = new ArrayList<>();
    }
}
