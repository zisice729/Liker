# 高并发点赞系统（XXL-Job 版）
基于既定设计规范，**替换 Spring 原生定时任务为 XXL-Job**，其余架构、存储、MQ、Lua、缓存逻辑保持不变；提供完整工程目录、配置、SQL、伪代码，可直接交付落地实现。

## 一、技术栈
SpringBoot 2.7 + Redis + Kafka + MySQL + MyBatis-Plus + Caffeine + Redisson + Redis Pub/Sub + **XXL-Job**

## 二、项目文件目录
```
like-system
├── pom.xml
├── src
│   ├── main
│   │   ├── java
│   │   │   └── com
│   │   │       └── like
│   │   │           ├── LikeApplication.java                # 启动类
│   │   │           ├── config                            # 配置类
│   │   │           │   ├── CaffeineConfig.java           # 本地缓存配置
│   │   │           │   ├── RedisConfig.java              # Redis+Lua+Pub/Sub
│   │   │           │   ├── KafkaConfig.java             # Kafka 配置
│   │   │           │   └── XxlJobConfig.java            # XXL-Job 客户端配置
│   │   │           ├── controller                        # 接口层
│   │   │           │   └── LikeController.java
│   │   │           ├── dto                              # 传输对象
│   │   │           │   ├── req
│   │   │           │   │   └── LikeOperateReq.java
│   │   │           │   └── resp
│   │   │           │       └── LikeResp.java
│   │   │           ├── entity                           # 数据库实体
│   │   │           │   ├── LikesRecord.java              # 点赞归档表
│   │   │           │   └── LikeCompensate.java          # 消息补偿表
│   │   │           ├── mapper                           # MyBatis Mapper
│   │   │           │   ├── LikesRecordMapper.java
│   │   │           │   └── LikeCompensateMapper.java
│   │   │           ├── mq                               # 消息队列
│   │   │           │   ├── dto
│   │   │           │   │   └── LikeKafkaMsg.java        # Kafka消息体
│   │   │           │   ├── producer
│   │   │           │   │   └── LikeProducer.java
│   │   │           │   └── consumer
│   │   │           │       └── LikeConsumer.java
│   │   │           ├── service                          # 业务服务
│   │   │           │   ├── LikeService.java
│   │   │           │   └── impl
│   │   │           │       └── LikeServiceImpl.java
│   │   │           ├── job                              # XXL-Job 任务类（替换原生定时）
│   │   │           │   ├── CompensateJob.java           # 补偿重试任务
│   │   │           │   └── DataCheckJob.java            # 数据对账任务
│   │   │           ├── util                             # 工具类
│   │   │           │   ├── ShardUtil.java               # Set分片计算
│   │   │           │   └── AlertUtil.java                # 告警工具
│   │   │           └── constant                         # 常量
│   │   │               ├── RedisKeyConst.java
│   │   │               └── CommonConst.java
│   │   └── resources
│   │       ├── application.yml                         # 主配置（含XXL-Job）
│   │       ├── lua
│   │       │   └── like_operate.lua                    # Redis Lua脚本
│   │       └── mybatis
│   │           └── mapper
│   │               ├── LikesRecordMapper.xml
│   │               └── LikeCompensateMapper.xml
│   └── test
└── README.md
```

## 三、数据库表结构（不变）
### 1. 点赞归档表 `likes_record`
```sql
CREATE TABLE likes_record (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    obj_id BIGINT NOT NULL COMMENT '业务对象ID(文章/评论)',
    user_id BIGINT NOT NULL COMMENT '用户ID',
    status TINYINT NOT NULL COMMENT '1-点赞 0-取消点赞',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_obj_user (obj_id, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='点赞冷数据归档表';
```

### 2. Kafka消费补偿表 `like_compensate`
```sql
CREATE TABLE like_compensate (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    msg_body JSON NOT NULL COMMENT '原始Kafka消息',
    retry_times INT NOT NULL DEFAULT 0 COMMENT '已重试次数',
    status TINYINT NOT NULL DEFAULT 1 COMMENT '1-待重试 2-成功 3-重试耗尽人工处理',
    error_msg VARCHAR(1000) COMMENT '异常信息',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status_retry (status, retry_times)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Kafka消费补偿表';
```

## 四、全局常量 & 工具类
### 1. CommonConst 通用常量
```java
public class CommonConst {
    // Set分片总数
    public static final int SHARD_COUNT = 100;
    // 消息最大自动重试次数
    public static final int MAX_RETRY = 3;
    // Caffeine 缓存过期时间(秒)
    public static final int CAFFEINE_EXPIRE = 5;
    // 冷数据判定天数
    public static final int COLD_DATA_DAY = 30;
    // 本地缓存失效 Pub/Sub 通道
    public static final String CACHE_INVALID_CHANNEL = "like_cache_invalid_channel";
    // XXL-Job 分布式锁Key
    public static final String JOB_LOCK_COMPENSATE = "like:job:compensate:lock";
    public static final String JOB_LOCK_CHECK = "like:job:check:lock";
}
```

### 2. RedisKeyConst Redis Key 常量
```java
public class RedisKeyConst {
    // 点赞总数 key: like:cnt:{objId}
    public static final String LIKE_COUNT_KEY = "like:cnt:%s";
    // 分片Set key: like:set:{objId}:{shardIndex}
    public static final String LIKE_SET_KEY = "like:set:%s:%s";
}
```

### 3. ShardUtil 分片工具
```java
@Component
public class ShardUtil {
    public int getShardIndex(Long userId) {
        return (int) (userId % CommonConst.SHARD_COUNT);
    }
}
```

### 4. AlertUtil 告警工具
```java
@Component
public class AlertUtil {
    // 对接钉钉/企业微信告警
    public void sendAlert(String content) {
        // 告警推送逻辑
    }
}
```

## 五、Redis Lua 脚本
路径：`resources/lua/like_operate.lua`
```lua
-- KEYS[1] 计数key , KEYS[2] 分片Set key
-- ARGV[1] userId
local countKey = KEYS[1]
local setKey = KEYS[2]
local userId = ARGV[1]

local isExist = redis.call('sismember', setKey, userId)
if isExist == 1 then
    -- 取消点赞
    redis.call('srem', setKey, userId)
    redis.call('decr', countKey)
else
    -- 新增点赞
    redis.call('sadd', setKey, userId)
    redis.call('incr', countKey)
end
return redis.call('get', countKey)
```

## 六、配置类
### 1. application.yml（整合 XXL-Job + Kafka + Redis）
```yaml
server:
  port: 8080

spring:
  # Redis
  redis:
    host: 127.0.0.1
    port: 6379
    password:
    database: 0
  # Kafka 关闭原生重试、指数退避，无死信队列
  kafka:
    bootstrap-servers: 127.0.0.1:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      retries: 2
      acks: 1
    consumer:
      group-id: like-consumer-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      enable-auto-commit: false
      max-poll-records: 50
      properties:
        retry.max.attempts: 0
        max.retry.backoff.ms: 0
        spring.json.trusted.packages: "*"

# XXL-Job 客户端配置
xxl:
  job:
    admin:
      addresses: http://127.0.0.1:8081/xxl-job-admin
    executor:
      appname: like-system-executor
      address:
      ip:
      port: 9999
      logpath: /data/xxl-job/logs
      logretentiondays: 7

# MyBatis-Plus
mybatis-plus:
  mapper-locations: classpath:mybatis/mapper/*.xml
  configuration:
    map-underscore-to-camel-case: true
```

### 2. CaffeineConfig 本地缓存
```java
@Configuration
public class CaffeineConfig {
    @Bean
    public LoadingCache<String, Long> likeCountCache(RedisTemplate<String, Object> redisTemplate) {
        return Caffeine.newBuilder()
                .expireAfterWrite(CommonConst.CAFFEINE_EXPIRE, TimeUnit.SECONDS)
                .maximumSize(100000)
                .build(key -> {
                    String val = redisTemplate.opsForValue().get(key);
                    return val == null ? 0L : Long.parseLong(val);
                });
    }
}
```

### 3. RedisConfig（Lua + Pub/Sub 缓存失效）
```java
@Configuration
public class RedisConfig {

    @Bean
    public DefaultRedisScript<Long> likeOperateScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/like_operate.lua")));
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public RedisMessageListenerContainer container(RedisConnectionFactory factory,
                                                   CacheInvalidListener listener) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        container.addMessageListener(listener, new ChannelTopic(CommonConst.CACHE_INVALID_CHANNEL));
        return container;
    }

    @Component
    class CacheInvalidListener implements MessageListener {
        @Autowired
        private LoadingCache<String, Long> likeCountCache;

        @Override
        public void onMessage(Message message, byte[] pattern) {
            String cacheKey = new String(message.getBody());
            likeCountCache.invalidate(cacheKey);
        }
    }
}
```

### 4. XxlJobConfig XXL-Job 客户端配置
```java
@Configuration
public class XxlJobConfig {

    @Value("${xxl.job.admin.addresses}")
    private String adminAddresses;
    @Value("${xxl.job.executor.appname}")
    private String appName;
    @Value("${xxl.job.executor.port}")
    private int port;
    @Value("${xxl.job.executor.logpath}")
    private String logPath;
    @Value("${xxl.job.executor.logretentiondays}")
    private int logRetentionDays;

    @Bean
    public XxlJobSpringExecutor xxlJobExecutor() {
        XxlJobSpringExecutor executor = new XxlJobSpringExecutor();
        executor.setAdminAddresses(adminAddresses);
        executor.setAppname(appName);
        executor.setPort(port);
        executor.setLogPath(logPath);
        executor.setLogRetentionDays(logRetentionDays);
        return executor;
    }
}
```

## 七、实体类 & MQ 消息体
### 1. Kafka 消息体 LikeKafkaMsg
```java
@Data
public class LikeKafkaMsg implements Serializable {
    private Long objId;
    private Long userId;
    private Integer action; // 1点赞 0取消
    private Long timestamp;
}
```

### 2. 数据库实体
```java
// 点赞归档表
@Data
@TableName("likes_record")
public class LikesRecord {
    private Long id;
    private Long objId;
    private Long userId;
    private Integer status;
    private LocalDateTime createTime;
}

// 补偿表
@Data
@TableName("like_compensate")
public class LikeCompensate {
    private Long id;
    private String msgBody;
    private Integer retryTimes;
    private Integer status;
    private String errorMsg;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
```

## 八、Mapper 层
```java
public interface LikesRecordMapper extends BaseMapper<LikesRecord> {
    @Insert("INSERT INTO likes_record(obj_id,user_id,status) VALUES(#{objId},#{userId},#{status}) " +
            "ON DUPLICATE KEY UPDATE status=#{status}")
    void insertOrUpdate(LikesRecord record);

    @Select("SELECT COUNT(1) FROM likes_record WHERE obj_id=#{objId} AND status=1")
    Long countLikeByObjId(Long objId);

    @Select("SELECT status FROM likes_record WHERE obj_id=#{objId} AND user_id=#{userId}")
    Integer getUserLikeStatus(Long objId, Long userId);
}

public interface LikeCompensateMapper extends BaseMapper<LikeCompensate> {
    @Select("SELECT * FROM like_compensate WHERE status=1 AND retry_times < #{maxRetry} LIMIT 300")
    List<LikeCompensate> selectWaitRetryData(@Param("maxRetry") Integer maxRetry);
}
```

## 九、Kafka 生产者 & 消费者
### 1. 生产者
```java
@Service
public class LikeProducer {
    private static final String TOPIC = "topic_like";
    @Autowired
    private KafkaTemplate<String, LikeKafkaMsg> kafkaTemplate;

    public void sendMsg(LikeKafkaMsg msg) {
        kafkaTemplate.send(TOPIC, msg).addCallback(
                result -> log.info("Kafka消息发送成功"),
                ex -> log.error("Kafka消息发送失败", ex)
        );
    }
}
```

### 2. 消费者（手动提交 offset，异常入补偿表）
```java
@Component
public class LikeConsumer {
    @Autowired
    private LikesRecordMapper recordMapper;
    @Autowired
    private LikeCompensateMapper compensateMapper;

    @KafkaListener(topics = "topic_like")
    public void listen(ConsumerRecords<String, LikeKafkaMsg> records, Acknowledgment ack) {
        for (ConsumerRecord<String, LikeKafkaMsg> record : records) {
            LikeKafkaMsg msg = record.value();
            try {
                LikesRecord po = new LikesRecord();
                po.setObjId(msg.getObjId());
                po.setUserId(msg.getUserId());
                po.setStatus(msg.getAction());
                recordMapper.insertOrUpdate(po);
            } catch (Exception e) {
                LikeCompensate compensate = new LikeCompensate();
                compensate.setMsgBody(JSON.toJSONString(msg));
                compensate.setRetryTimes(0);
                compensate.setStatus(1);
                compensate.setErrorMsg(e.getMessage());
                compensateMapper.insert(compensate);
            }
        }
        ack.acknowledge();
    }
}
```

## 十、核心业务 Service
### 1. 接口
```java
public interface LikeService {
    Long operateLike(Long objId, Long userId);
    Long getLikeCount(Long objId);
    Boolean checkUserLiked(Long objId, Long userId);
}
```

### 2. 实现类
```java
@Service
public class LikeServiceImpl implements LikeService {
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private DefaultRedisScript<Long> likeOperateScript;
    @Autowired
    private LoadingCache<String, Long> likeCountCache;
    @Autowired
    private ShardUtil shardUtil;
    @Autowired
    private LikeProducer likeProducer;
    @Autowired
    private LikesRecordMapper recordMapper;
    @Autowired
    private RedisTemplate<String, Object> pubRedisTemplate;

    @Override
    public Long operateLike(Long objId, Long userId) {
        int shardIdx = shardUtil.getShardIndex(userId);
        String countKey = String.format(RedisKeyConst.LIKE_COUNT_KEY, objId);
        String setKey = String.format(RedisKeyConst.LIKE_SET_KEY, objId, shardIdx);

        // Lua 原子执行点赞/取消
        Long newCount = redisTemplate.execute(
                likeOperateScript,
                Arrays.asList(countKey, setKey),
                userId.toString()
        );

        // 本地缓存 + 集群广播失效
        likeCountCache.invalidate(countKey);
        pubRedisTemplate.convertAndSend(CommonConst.CACHE_INVALID_CHANNEL, countKey);

        // 发送Kafka消息
        LikeKafkaMsg msg = new LikeKafkaMsg();
        msg.setObjId(objId);
        msg.setUserId(userId);
        msg.setTimestamp(System.currentTimeMillis());
        // 简化：可根据点赞状态赋值 action
        likeProducer.sendMsg(msg);

        return newCount;
    }

    @Override
    public Long getLikeCount(Long objId) {
        String countKey = String.format(RedisKeyConst.LIKE_COUNT_KEY, objId);
        try {
            return likeCountCache.get(countKey);
        } catch (Exception e) {
            // Redis 故障降级 MySQL
            return recordMapper.countLikeByObjId(objId);
        }
    }

    @Override
    public Boolean checkUserLiked(Long objId, Long userId) {
        int shardIdx = shardUtil.getShardIndex(userId);
        String setKey = String.format(RedisKeyConst.LIKE_SET_KEY, objId, shardIdx);
        try {
            Boolean exist = redisTemplate.opsForSet().isMember(setKey, userId);
            return exist != null && exist;
        } catch (Exception e) {
            // 降级 MySQL
            Integer status = recordMapper.getUserLikeStatus(objId, userId);
            return status != null && status == 1;
        }
    }
}
```

## 十一、Controller 接口
```java
@RestController
@RequestMapping("/api/like")
public class LikeController {
    @Autowired
    private LikeService likeService;

    @PostMapping("/operate")
    public R<Long> operate(@RequestParam Long objId, @RequestParam Long userId) {
        return R.ok(likeService.operateLike(objId, userId));
    }

    @GetMapping("/count")
    public R<Long> getCount(@RequestParam Long objId) {
        return R.ok(likeService.getLikeCount(objId));
    }

    @GetMapping("/check")
    public R<Boolean> check(@RequestParam Long objId, @RequestParam Long userId) {
        return R.ok(likeService.checkUserLiked(objId, userId));
    }
}
```

## 十二、XXL-Job 任务类（核心改动：替换 Spring Task）
### 1. 补偿重试任务 CompensateJob
> 调度规则：XXL-Job 控制台配置 `Cron: 0/1 * * * * ?` 每分钟执行
```java
@Component
public class CompensateJob {
    @Autowired
    private LikeCompensateMapper compensateMapper;
    @Autowired
    private LikesRecordMapper recordMapper;
    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    private AlertUtil alertUtil;

    @XxlJob("likeCompensateJobHandler")
    public void execute() throws Exception {
        RLock lock = redissonClient.getLock(CommonConst.JOB_LOCK_COMPENSATE);
        try {
            // 分布式锁，防止集群多实例并行执行
            if (!lock.tryLock(0, 70, TimeUnit.SECONDS)) {
                return;
            }
            List<LikeCompensate> waitList = compensateMapper.selectWaitRetryData(CommonConst.MAX_RETRY);
            if (CollectionUtils.isEmpty(waitList)) {
                return;
            }
            for (LikeCompensate item : waitList) {
                LikeKafkaMsg msg = JSON.parseObject(item.getMsgBody(), LikeKafkaMsg.class);
                try {
                    LikesRecord po = new LikesRecord();
                    po.setObjId(msg.getObjId());
                    po.setUserId(msg.getUserId());
                    po.setStatus(msg.getAction());
                    recordMapper.insertOrUpdate(po);
                    item.setStatus(2);
                } catch (Exception e) {
                    item.setRetryTimes(item.getRetryTimes() + 1);
                    item.setErrorMsg(e.getMessage());
                    // 重试次数耗尽，标记人工处理并告警
                    if (item.getRetryTimes() >= CommonConst.MAX_RETRY) {
                        item.setStatus(3);
                        alertUtil.sendAlert("点赞消息重试次数用尽，请人工处理");
                    }
                }
                compensateMapper.updateById(item);
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
```

### 2. 数据对账任务 DataCheckJob
> 调度规则：XXL-Job 控制台配置 `Cron: 0 0 * * * ?` 每小时执行
```java
@Component
public class DataCheckJob {
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private LikesRecordMapper recordMapper;
    @Autowired
    private RedissonClient redissonClient;

    @XxlJob("likeDataCheckJobHandler")
    public void execute() throws Exception {
        RLock lock = redissonClient.getLock(CommonConst.JOB_LOCK_CHECK);
        try {
            if (!lock.tryLock(0, 3700, TimeUnit.SECONDS)) {
                return;
            }
            // 增量/抽样获取待校验对象ID
            List<Long> objIdList = getNeedCheckObjId();
            for (Long objId : objIdList) {
                String countKey = String.format(RedisKeyConst.LIKE_COUNT_KEY, objId);
                Object redisVal = redisTemplate.opsForValue().get(countKey);
                Long redisCount = redisVal == null ? 0L : Long.parseLong(redisVal.toString());
                Long mysqlCount = recordMapper.countLikeByObjId(objId);

                // 数据不一致，以Redis为准修复MySQL
                if (!redisCount.equals(mysqlCount)) {
                    fixMysqlData(objId, redisCount);
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private List<Long> getNeedCheckObjId() {
        // 业务实现：增量/抽样查询待对账objId
        return new ArrayList<>();
    }

    private void fixMysqlData(Long objId, Long standardCount) {
        // 业务实现：修正MySQL数据
    }
}
```

## 十三、XXL-Job 控制台配置说明
1. **执行器**
   - 执行器名称：`like-system-executor`
   - 注册方式：自动注册
2. **任务配置**
   - 任务1（补偿重试）
     - 任务Handler：`likeCompensateJobHandler`
     - Cron表达式：`0/1 * * * * ?` 每分钟执行
   - 任务2（数据对账）
     - 任务Handler：`likeDataCheckJobHandler`
     - Cron表达式：`0 0 * * * ?` 每小时执行
3. 运行模式：默认 **BEAN 模式**

## 十四、核心强制约束（编码&运维）
1. Redis 开启 AOF，内存策略 `noeviction`；
2. 点赞操作必须走 Lua 原子脚本，禁止 Java 分步操作；
3. Kafka 全程关闭原生重试、指数退避、不使用死信队列；
4. 定时任务统一使用 **XXL-Job 托管**，禁用 Spring @Scheduled；
5. 补偿表最大自动重试 3 次，超限触发告警人工介入；
6. 本地缓存通过 Redis Pub/Sub 集群失效，保证多实例一致性；
7. 对账以 Redis 为权威数据源，只修复 MySQL，不修改 Redis；
8. 所有定时任务通过 Redisson 分布式锁控制，避免集群重复执行。