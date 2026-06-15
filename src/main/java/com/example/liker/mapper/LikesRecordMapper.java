package com.example.liker.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.liker.entity.LikesRecord;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * 点赞归档表Mapper接口
 */
public interface LikesRecordMapper extends BaseMapper<LikesRecord> {

    /**
     * 插入或更新点赞记录
     * 使用ON DUPLICATE KEY UPDATE实现幂等性
     *
     * @param record 点赞记录
     */
    @Insert("INSERT INTO likes_record(obj_id,user_id,status) VALUES(#{objId},#{userId},#{status}) " +
            "ON DUPLICATE KEY UPDATE status=#{status}")
    void insertOrUpdate(LikesRecord record);

    /**
     * 根据业务对象ID统计有效点赞数
     *
     * @param objId 业务对象ID
     * @return 点赞数量
     */
    @Select("SELECT COUNT(1) FROM likes_record WHERE obj_id=#{objId} AND status=1")
    Long countLikeByObjId(Long objId);

    /**
     * 查询用户对指定对象的点赞状态
     *
     * @param objId 业务对象ID
     * @param userId 用户ID
     * @return 点赞状态：1-已点赞，0-未点赞，null-无记录
     */
    @Select("SELECT status FROM likes_record WHERE obj_id=#{objId} AND user_id=#{userId}")
    Integer getUserLikeStatus(@Param("objId") Long objId, @Param("userId") Long userId);
}