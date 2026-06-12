
package com.example.liker.repository.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.liker.repository.entity.LikeMsgCompensate;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * 补偿消息Mapper接口
 */
@Mapper
public interface LikeMsgCompensateMapper extends BaseMapper<LikeMsgCompensate> {

    /**
     * 查询待重试的补偿记录
     */
    List<LikeMsgCompensate> selectPendingRetry(@Param("limit") Integer limit);

    /**
     * 批量更新状态
     */
    void batchUpdateStatus(@Param("ids") List<Long> ids, @Param("status") Integer status);

    /**
     * 批量更新重试次数
     */
    void batchUpdateRetryCount(@Param("ids") List<Long> ids, @Param("retryCount") Integer retryCount, @Param("status") Integer status);
}
