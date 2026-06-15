# 高并发点赞系统 (Liker)

## 项目简介
基于 SpringBoot 2.7 + Redis + Kafka + XXL-Job + ShardingSphere + MyBatis-Plus 的高并发点赞系统后端。

## 技术栈
- SpringBoot 2.7.18
- MyBatis-Plus 3.5.3.2
- Redisson 3.23.3（Redis 客户端）
- Kafka（异步消息）
- XXL-Job 2.4.0（分布式定时任务）
- Caffeine（本地缓存）
- ShardingSphere 5.3.2（分库分表）

## 核心特性
- **超高性能读**：Redis 主存 + Caffeine 本地缓存，多级缓存架构
- **高并发写**：Redis Lua 脚本保证点赞操作原子性
- **数据最终一致性**：Kafka 异步同步 + 三级兜底（指数退避重试 / 补偿表 / 定时对账）
- **防刷限流**：基于 Redisson RRateLimiter 的分布式限流
- **分布式任务调度**：使用 XXL-Job 替代 Spring @Scheduled

## 项目结构
```
src/main/java/com/example/liker/
├── common/                # 公共模块
│   ├── constants/         # 常量
│   ├── exception/         # 异常
│   ├── lock/              # 分布式锁模板
│   └── util/              # Redis 工具类
├── config/                # 配置类
├── controller/            # 控制器
│   └── advice/            # 全局异常处理
├── dto/                   # 数据传输对象
│   ├── request/           # 请求 DTO
│   └── response/          # 响应 DTO
├── job/                   # XXL-Job 任务处理器
├── mq/                    # 消息队列
│   ├── consumer/          # 消费者
│   ├── msg/               # 消息体
│   └── producer/          # 生产者
├── repository/            # 数据访问层
│   ├── do/                # DO 实体
│   ├── entity/            # 普通实体
│   └── mapper/            # Mapper 接口
├── service/               # 业务层
│   └── impl/              # 业务实现
└── LikerApplication.java  # 启动类
```

## 核心流程
1. **点赞/取消点赞**：参数校验 → 限流检查 → Redis Lua 原子操作 → 失效本地缓存 → 发送 Kafka 消息 → 返回结果
2. **Kafka 消费**：一级重试（指数退避）→ 二级补偿表 → 三级定时对账
3. **定时任务**：补偿重试（每 30 秒） + Redis-MySQL 对账（每小时）

## 启动方式
1. 启动 Redis、Kafka、XXL-Job Admin
2. 执行 `schema.sql` 初始化数据库
3. 运行 `LikerApplication.java`

## 配置说明
所有业务配置在 `application.yml` 的 `liker.*` 节点下。
