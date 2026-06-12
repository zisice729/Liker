
package com.example.liker.service;

/**
 * 补偿服务接口
 */
public interface CompensateService {

    /**
     * 处理待补偿的记录
     * @return int[] [成功数, 失败数, 人工处理数]
     */
    int[] processPendingCompensate();
}
