
package com.example.liker.service;

/**
 * 对账服务接口
 */
public interface ReconcileService {

    /**
     * 执行Redis-MySQL对账
     * @return int[] [检查条数, 不一致条数]
     */
    int[] doReconcile();
}
