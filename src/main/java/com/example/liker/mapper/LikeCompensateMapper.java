package com.example.liker.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.liker.entity.LikeCompensate;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * Kafka消费补偿表Mapper接口
 */
public interface LikeCompensateMapper extends BaseMapper<LikeCompensate> {

    /**
     * 查询待重试的补偿消息列表
     *
     * @param maxRetry 最大重试次数
     * @return 待重试消息列表（最多300条）
     */
    @Select("SELECT * FROM like_compensate WHERE status=1 AND retry_times < #{maxRetry} LIMIT 300")
    List<LikeCompensate> selectWaitRetryData(@Param("maxRetry") Integer maxRetry);
}