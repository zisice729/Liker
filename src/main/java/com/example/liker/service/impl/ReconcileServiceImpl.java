
package com.example.liker.service.impl;

import com.example.liker.config.RedisConfig;
import com.example.liker.common.constants.LikeConstant;
import com.example.liker.common.lock.RedisLockTemplate;
import com.example.liker.repository.do.UserLikeDO;
import com.example.liker.repository.mapper.UserLikeMapper;
import com.example.liker.service.ReconcileService;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReconcileServiceImpl implements ReconcileService {

    private final RedissonClient redissonClient;
    private final UserLikeMapper userLikeMapper;
    private final RedisLockTemplate redisLockTemplate;

    @Qualifier("likeCountCache")
    private final Cache<String, Long> likeCountCache;

    @Qualifier("emptyValueCache")
    private final Cache<String, Boolean> emptyValueCache;

    @Value("${liker.task.reconcile.lock-duration-ms:60000}")
    private long lockDurationMs;

    private static final int ALERT_THRESHOLD = 100;

    @Override
    public int[] doReconcile() {
        log.debug("开始执行Redis-MySQL对账任务");

        Integer[] result = redisLockTemplate.executeWithLock(
                LikeConstant.LOCK_KEY_RECONCILE,
                0, lockDurationMs / 1000,
                this::doReconcileInternal
        );

        if (result == null) {
            log.debug("对账任务已在其他节点执行");
            return new int[]{0, 0};
        }

        return new int[]{result[0], result[1]};
    }

    private Integer[] doReconcileInternal() {
        List<int[]> bizList = getRecentBizList();
        int totalCount = bizList.size();

        int inconsistentCount = reconcileBizList(bizList);

        if (inconsistentCount > ALERT_THRESHOLD) {
            sendAlert(inconsistentCount);
        }

        log.info("对账任务完成: 检查{}条数据, 不一致{}条", totalCount, inconsistentCount);
        return new Integer[]{totalCount, inconsistentCount};
    }

    private List<int[]> getRecentBizList() {
        List<int[]> list = new ArrayList<>();
        Set<String> keys = redissonClient.getKeys().getKeysByPattern(LikeConstant.REDIS_KEY_LIKE_PREFIX + "*");
        
        for (String key : keys) {
            try {
                String[] parts = key.split(":");
                if (parts.length == 3) {
                    int bizType = Integer.parseInt(parts[1]);
                    long bizId = Long.parseLong(parts[2]);
                    list.add(new int[]{bizType, (int) bizId});
                }
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        
        return list;
    }

    private int reconcileBizList(List<int[]> bizList) {
        int inconsistentCount = 0;

        for (int[] biz : bizList) {
            Integer bizType = biz[0];
            Long bizId = (long) biz[1];

            try {
                if (isDataInconsistent(bizType, bizId)) {
                    fixData(bizType, bizId);
                    inconsistentCount++;
                }
            } catch (Exception e) {
                log.error("对账失败: bizType={}, bizId={}, error={}", bizType, bizId, e.getMessage());
            }
        }

        return inconsistentCount;
    }

    private boolean isDataInconsistent(Integer bizType, Long bizId) {
        long redisCount = getRedisLikeCount(bizType, bizId);
        Long dbCount = userLikeMapper.countValidLikes(bizType, bizId);

        boolean inconsistent = !Long.valueOf(redisCount).equals(dbCount);
        
        if (inconsistent) {
            log.warn("数据不一致: bizType={}, bizId={}, redisCount={}, dbCount={}",
                    bizType, bizId, redisCount, dbCount);
        }

        return inconsistent;
    }

    private long getRedisLikeCount(Integer bizType, Long bizId) {
        String likeKey = RedisConfig.buildLikeKey(bizType, bizId);
        return redissonClient.getScoredSortedSet(likeKey).size();
    }

    private void fixData(Integer bizType, Long bizId) {
        try {
            String likeKey = RedisConfig.buildLikeKey(bizType, bizId);
            RScoredSortedSet<String> sortedSet = redissonClient.getScoredSortedSet(likeKey);
            Collection<String> userIds = sortedSet.readAll();

            if (!userIds.isEmpty()) {
                List<UserLikeDO> userLikeList = new ArrayList<>();
                for (String userId : userIds) {
                    UserLikeDO userLike = UserLikeDO.builder()
                            .bizType(bizType)
                            .bizId(bizId)
                            .userId(Long.parseLong(userId))
                            .operateType(LikeConstant.OPERATE_TYPE_LIKE)
                            .operateTime(LocalDateTime.now())
                            .build();
                    userLikeList.add(userLike);
                }

                userLikeMapper.batchInsert(userLikeList);
                
                String cacheKey = bizType + ":" + bizId;
                likeCountCache.invalidate(cacheKey);
                emptyValueCache.invalidate(RedisConfig.buildEmptyValueKey(bizType, bizId));

                log.info("数据修正完成: bizType={}, bizId={}", bizType, bizId);
            }
        } catch (Exception e) {
            log.error("数据修正失败: {}", e.getMessage());
        }
    }

    private void sendAlert(int inconsistentCount) {
        try {
            log.error("对账发现大量数据不一致，数量: {}", inconsistentCount);
        } catch (Exception e) {
            log.error("发送对账告警失败: {}", e.getMessage());
        }
    }
}
