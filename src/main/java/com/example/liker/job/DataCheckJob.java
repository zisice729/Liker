package com.example.liker.job;

import com.example.liker.constant.CommonConst;
import com.example.liker.constant.RedisKeyConst;
import com.example.liker.mapper.LikesRecordMapper;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * XXL-Job 数据对账任务
 * 负责校验Redis和MySQL中的点赞数据一致性
 * 以Redis为权威数据源，修复MySQL数据
 * 
 * 调度配置：
 * - Job Handler: likeDataCheckJobHandler
 * - Cron: 0 0 * * * ? (每小时执行)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DataCheckJob {

    private final RedisTemplate<String, Object> redisTemplate;
    private final LikesRecordMapper recordMapper;
    private final RedissonClient redissonClient;

    /**
     * 执行数据对账任务
     * 1. 获取分布式锁防止集群重复执行
     * 2. 获取待校验的对象ID列表
     * 3. 比对Redis和MySQL中的点赞数
     * 4. 数据不一致时以Redis为准修复MySQL
     */
    @XxlJob("likeDataCheckJobHandler")
    public void execute() throws Exception {
        RLock lock = redissonClient.getLock(CommonConst.JOB_LOCK_CHECK);
        try {
            // 获取分布式锁，最多等待0秒，持有3700秒（超过1小时）
            if (!lock.tryLock(0, 3700, TimeUnit.SECONDS)) {
                XxlJobHelper.log("获取分布式锁失败，跳过本次执行");
                return;
            }
            
            // 获取待校验的对象ID列表
            List<Long> objIdList = getNeedCheckObjId();
            int fixCount = 0;
            
            // 逐条对账
            for (Long objId : objIdList) {
                String countKey = String.format(RedisKeyConst.LIKE_COUNT_KEY, objId);
                Object redisVal = redisTemplate.opsForValue().get(countKey);
                Long redisCount = redisVal == null ? 0L : Long.parseLong(redisVal.toString());
                Long mysqlCount = recordMapper.countLikeByObjId(objId);

                // 数据不一致，以Redis为准修复MySQL
                if (!redisCount.equals(mysqlCount)) {
                    fixMysqlData(objId, redisCount);
                    fixCount++;
                }
            }
            
            XxlJobHelper.handleSuccess("数据对账完成，检查数量: " + objIdList.size() + ", 修复数量: " + fixCount);
        } finally {
            // 释放锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * 获取需要对账的对象ID列表
     * 可实现增量查询或抽样查询策略
     *
     * @return 对象ID列表
     */
    private List<Long> getNeedCheckObjId() {
        // TODO: 实现增量/抽样查询逻辑
        return new ArrayList<>();
    }

    /**
     * 修复MySQL数据
     * 以Redis数据为准，修正MySQL中的点赞记录
     *
     * @param objId 业务对象ID
     * @param standardCount 标准点赞数（来自Redis）
     */
    private void fixMysqlData(Long objId, Long standardCount) {
        log.info("修复MySQL数据: objId={}, standardCount={}", objId, standardCount);
        // TODO: 实现数据修复逻辑
    }
}