
package com.example.liker.repository.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.liker.repository.do.UserLikeDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * 用户点赞Mapper接口
 */
@Mapper
public interface UserLikeMapper extends BaseMapper<UserLikeDO> {

    /**
     * 插入或更新点赞记录
     */
    void insertOrUpdate(@Param("userLike") UserLikeDO userLike);

    /**
     * 统计有效点赞数（只统计点赞操作）
     */
    Long countValidLikes(@Param("bizType") Integer bizType, @Param("bizId") Long bizId);

    /**
     * 批量插入
     */
    void batchInsert(@Param("list") List<UserLikeDO> list);
}
