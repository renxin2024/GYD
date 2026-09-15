# GYD — Get Your Distributed

**GYD（Get Your Distributed）** 系列文章的配套代码仓库，主题是**分布式系统里的各种解决方案**——消息队列的可靠投递、分布式事务、分布式锁、一致性协议、限流熔断、缓存、服务发现、幂等设计等。

与 [GYA](https://github.com/renxin2024/GYA)（Get Your Agent，Python）和 [GYA-Java](https://github.com/renxin2024/GYA-Java)（Get Your Agent，Java）是同一套命名血脉，但**独立成系列**，不复用它们的代码目录。

## 版本要求

| 项 | 要求 |
|----|------|
| JDK | 21（LTS）—— `java -version` 显示 `21.0.x` |
| 构建 | **Gradle Wrapper 固定 8.14.2**（仓库自带 `gradlew`，无需预装 Gradle） |
| 框架 | **Spring Boot 4.1.1**（基于 Spring Framework 7.0，Java 17 基线） |
| 消息队列 | **RabbitMQ 4.3.5**（Docker 运行，见「本地环境」一节） |
| 包名 | `cn.renxinblog.gyd.*` |

Gradle 8.14.2 是 Spring Boot 4 的最低兼容线（Boot 4 要求 Gradle 8.14+ 或 9.x），本仓库固定在这一版。

安装参考：

- macOS: `brew install openjdk@21`
- Ubuntu/Debian: `sudo apt install openjdk-21-jdk-headless`
- Windows: [Adoptium](https://adoptium.net)（JDK 21）

## 国内下载源说明

| 用途 | 源 |
|------|-----|
| Gradle 发行版 | 腾讯云镜像（`gradle-wrapper.properties` 已配置） |
| Gradle 插件 | 阿里云 `gradle-plugin` 镜像（`settings.gradle.kts` 的 `pluginManagement`） |
| Maven 依赖 | 阿里云 `public` + `central` 镜像（根 `build.gradle.kts` 的 `allprojects`） |

无需科学上网。

## 目录导航

| 目录 | 对应文章 | 主题 |
|------|---------|------|
| [`rabbitmq-architecture`](rabbitmq-architecture) | 前置篇 | RabbitMQ 的架构与特性：一条消息的一生 |
| [`c01-mq-reliable-delivery`](c01-mq-reliable-delivery) | 第 1 篇 | 消息队列的可靠投递与可靠消费 |
| [`c02-reconciliation`](c02-reconciliation) | 第 2 篇 | 跨行清算对账：三方核对、差异分级、冲正 |
| [`c03-rocketmq-architecture`](c03-rocketmq-architecture) | 第 3 篇 | RocketMQ 的架构与机制：顺序、定时、事务、过滤是同一份 CommitLog 上的四个投影 |
| [`infra`](infra) | — | 系列共享中间件（RabbitMQ、RocketMQ、PostgreSQL 等，不属于任何单篇） |

`cXX-<slug>/` 只放**单篇文章的演示代码**；RabbitMQ、Redis、Kafka 这类被多篇复用的组件放仓库级的 [`infra/`](infra)，避免归属错乱和端口冲突。前置篇的目录 [`rabbitmq-architecture/`](rabbitmq-architecture) 刻意不带 `cXX-` 前缀——它排在编号文章之前，在系列里没有序号。

## 本地环境（RabbitMQ）

RabbitMQ 是**系列共享的中间件**，不隶属于任何单篇文章，配置见 [`infra/rabbitmq/docker-compose.yml`](infra/rabbitmq/docker-compose.yml)：

```bash
cd infra/rabbitmq
docker compose up -d         # 启动
docker compose down          # 停止（数据保留在命名卷中）
docker compose down -v       # 停止并清空数据
```

| 项 | 值 |
|----|-----|
| 版本 | RabbitMQ **4.3.5**（Erlang/OTP 27，官方镜像原生支持 arm64） |
| AMQP | `amqp://admin:admin123@localhost:5672` |
| 管理控制台 | http://localhost:15672 （`admin` / `admin123`） |
| 数据卷 | `gyd-rabbitmq-data`，`docker compose down` 不会丢数据 |

`admin` / `admin123` 只是本地开发默认凭据，**不要用于任何生产环境**。

另有一套 **3 节点集群**（`infra/rabbitmq-cluster/`，端口 5673–5675 / 15673–15675，与上面这套错开，可同时运行），用于验证 quorum queue 的多副本与多数派行为。搭建步骤与实测脚本见 [`infra/README.md`](infra/README.md)。

国内直连 Docker Hub 拉取镜像会超时，可先从镜像源拉取再打回官方 tag：

```bash
docker pull docker.m.daocloud.io/library/rabbitmq:4.3.5-management
docker tag  docker.m.daocloud.io/library/rabbitmq:4.3.5-management rabbitmq:4.3.5-management
```

## 快速开始

```bash
# 1. 先起 RabbitMQ（见上一节）
cd infra/rabbitmq && docker compose up -d && cd ../..

# 2. 编译 + 跑测试
./gradlew build

# 3. 只启动第 1 篇的演示应用
./gradlew :c01-mq-reliable-delivery:bootRun
```

启动后有五个入口可以手工验证可靠的各个环节：

```bash
# 自检
curl http://localhost:8080/gyd/c01/ping
# {"status":"ok"}

# 1) 正常订单 → 被消费、ack，队列归零
curl -X POST "http://localhost:8080/gyd/c01/order?orderId=order-1001&amount=199"

# 2) 会失败的订单（orderId 含 "fail"）→ nack 进重试队列，每 2s 重试一次，
#    3 次重试用尽后转入死信队列，全程约 6s
curl -X POST "http://localhost:8080/gyd/c01/order?orderId=order-fail-1002&amount=299"

# 3) 路由到无人绑定的键 → 触发 return 回调，但 confirm 仍返回 ack
#    （消息实际被丢弃，这是最容易被忽略的「静默丢消息」）
curl -X POST "http://localhost:8080/gyd/c01/order/unroutable?orderId=order-3001"

# 4) 持久化探针：分别投一条持久化 / 非持久化消息到无消费者的队列，
#    重启 broker 后对比数量
curl -X POST "http://localhost:8080/gyd/c01/order/probe?id=p1&persistent=true"
curl -X POST "http://localhost:8080/gyd/c01/order/probe?id=t1&persistent=false"

# 5) 批量投递慢消息（orderId 前缀 slow，每条处理 200ms），用于观测 prefetch
curl -X POST "http://localhost:8080/gyd/c01/order/burst?prefix=slow&count=50"
```

观察队列状态：

```bash
docker exec gyd-rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged consumers
docker exec gyd-rabbitmq rabbitmqctl list_queues name messages messages_persistent
```

## 消息拓扑

```
正常路径
  producer → order.exchange --order.created--> order.queue → consumer --ack--> 完成

失败路径（broker 驱动的延迟重试）
  consumer --nack(requeue=false)--> order.retry.exchange --order.retry--> order.retry.queue
         --TTL 2000ms 到期--> order.exchange --order.created--> order.queue

重试用尽（应用显式投递）
  consumer --publish--> order.dlx --order.dead--> order.dlq

探针（无消费者，仅用于持久化实验）
  producer → 默认交换机 --> gyd.c01.persist.probe
```

重试那一跳必须由 broker 的死信机制完成。原因是**只有被死信过的消息才会被 broker 追加 `x-death` 头**，而消费者正是靠它数出重试次数；若改成应用自己重发，新消息不带这个头，计数就断了。

重试用尽后也**不能**用 `basicNack` 收尾——本队列的死信出口是重试交换机，nack 只会让它再次进入重试循环；必须显式投到最终死信交换机再 ack。

## 实测记录（都是真跑出来的）

以下每一项都可以复现：1～3 走上面的 curl 入口，4～5 走 `infra/` 下的实测脚本。

### 1. 延迟重试与 `x-death` 计数

投递 `order-fail-retry-2` 后的真实日志：

```
09:37:00.946  处理失败，2000ms 后重试（第 1/3 次）  x-death=(无)
09:37:02.950  处理失败，2000ms 后重试（第 2/3 次）  x-death=retry.queue/expired ×1 + order.queue/rejected ×1
09:37:04.953  处理失败，2000ms 后重试（第 3/3 次）  x-death=retry.queue/expired ×2 + order.queue/rejected ×2
09:37:06.956  重试 3 次仍失败，转入死信队列（累计投递 4 次）
09:37:06.959  收到死信，累计处理 4 次
```

相邻两次的间隔是 2004ms / 2003ms / 2003ms，与 `x-message-ttl=2000` 吻合。

`x-death` 由 broker 维护，每次死信累加对应条目：

```
x-death=[
  {reason=expired,  count=2, queue=gyd.c01.order.retry.queue, routing-keys=[order.retry]},
  {reason=rejected, count=2, queue=gyd.c01.order.queue,       routing-keys=[order.created]}
]
```

只统计 `reason=rejected` 的那条——同一次失败会在重试队列（TTL 到期）和业务队列（被拒绝）各留一条记录，全算上会重复计数。

> 实现提示：`@Header("x-death")` 参数实测取不到值（绑定到的是原始 `Message` 对象），改为 `@Headers Map<String,Object>` 整表读取。

### 2. 持久化：`confirm` 的 ack 不等于已落盘

对无消费者的队列 `gyd.c01.persist.probe` 投递两条消息，**两条都收到 `confirm` 的 ack**：

```
[producer] 已投递持久化探针: id=probe-persistent, deliveryMode=PERSISTENT(2)
[confirm]  Broker 已确认消息: id=probe-persistent
[producer] 已投递持久化探针: id=probe-transient,  deliveryMode=NON_PERSISTENT(1)
[confirm]  Broker 已确认消息: id=probe-transient
```

`docker restart gyd-rabbitmq` 之后：

| | 重启前 | 重启后 |
|---|---|---|
| `messages` | 2 | **1** |
| `messages_persistent` | 1 | 1 |

非持久化那条**无声消失**。再单独只发一条非持久化消息验证：重启前 `messages=1`，重启后 `messages=0`。

即：消息要真正挺过 broker 重启，需要**交换机 durable + 队列 durable + 消息 deliveryMode=2** 三条同时成立，缺任何一条，`confirm` 的 ack 都照样会回，但消息已经不在了。

### 3. prefetch 决定「崩溃时会重投多少条」

50 条慢消息（每条处理 200ms），只改 `prefetch`，采样队列的未确认数：

| prefetch | `messages_unacknowledged` 峰值 | 排空耗时 |
|---|---|---|
| 1 | **1** | ~10s |
| 20 | **20** | ~10s |

两次吞吐基本相同——瓶颈在消费处理本身，不在网络往返。差别在于**同时在途未确认的消息数**：若消费者在这一刻崩溃，`prefetch=1` 最多重投 1 条，`prefetch=20` 最多重投 20 条。这就是「提高并行度」在可靠消费上要付的代价。

复现方式（不改配置文件，用环境变量覆盖）：

```bash
SPRING_RABBITMQ_LISTENER_SIMPLE_PREFETCH=1  ./gradlew :c01-mq-reliable-delivery:bootRun
SPRING_RABBITMQ_LISTENER_SIMPLE_PREFETCH=20 ./gradlew :c01-mq-reliable-delivery:bootRun
```

### 4. quorum queue：多数派在线才能写

单机持久化只挡得住进程重启，挡不住磁盘损坏和整机故障。要扛机器级故障只能用 quorum queue（基于 Raft 的多副本）。这套行为在单节点上无法验证，用 `infra/rabbitmq-cluster/` 起的 3 节点集群实测：

```
[STEP 1] 声明 quorum queue
         副本组 members = [rabbit@rabbitmq1, rabbit@rabbitmq2, rabbit@rabbitmq3]
         leader = rabbit@rabbitmq1
[STEP 2] 投 3 条持久化消息，全部收到 broker confirm
[STEP 3] 停掉 leader 所在节点 rabbit@rabbitmq1
         → 重新选主为 rabbit@rabbitmq2，在线副本 2/3
[STEP 4] 2/3 在线：投递 2 条成功；取回全部 5 条
         ['seed','seed','seed','after-leader-down','after-leader-down']
         ↑ 原 3 条一条不少，数据确实在多数派副本上
[STEP 5] 再停 rabbit@rabbitmq3 → 在线副本 1/3
[STEP 6] 失去多数派：投递被拒，15.0s 后 NACK（0 message(s) NACKed）
[STEP 7] 恢复节点 → 队列自愈，3/3 在线，投递恢复正常
```

结论：quorum queue 的可用性取决于**多数派是否在线**——2/3 在线照常读写，1/3 在线直接拒绝写入。这不是「消息丢了」，而是「没有多数派就不许写」，正是 Raft 避免脑裂的取舍。另有一个附带观察：失去多数派时连 `list_queues` 读队列状态本身都会超时。

复现（需要 3 节点集群，见 [`infra/README.md`](infra/README.md)）：

```bash
cd infra/rabbitmq-cluster && docker compose up -d && ./join-cluster.sh
python3 scripts/quorum-majority.py
```

### 5. per-message TTL：过期时机由队头决定

队列级 `x-message-ttl` 只能给整条队列一个延迟。要做「1s / 10s / 60s 递增退避」，直觉上会给每条消息设置各自的 `expiration`。实测这条路走不通：

```
实验 A  入队顺序 6000ms → 1000ms → 1000ms
        A-long    ttl=6000ms   实际死信于 +6.011s
        A-short1  ttl=1000ms   实际死信于 +6.011s   ← 本该 1s 走，被卡了 5 秒
        A-short2  ttl=1000ms   实际死信于 +6.011s   ← 同上
实验 B  入队顺序 1000ms → 1000ms → 6000ms
        B-short1  ttl=1000ms   实际死信于 +1.004s
        B-short2  ttl=1000ms   实际死信于 +1.004s
        B-long    ttl=6000ms   实际死信于 +6.004s
```

同样三条消息，只换入队顺序，结果完全不同。原因是**过期检查只在队头进行**：队头那条没过期，后面已经到期的消息也不能被丢弃或死信，只能一起等。

所以本 demo 的重试队列只能用固定间隔（2000ms）。要按消息各自延迟，只能每级延迟一个队列，或者上延迟插件。

复现：

```bash
python3 infra/rabbitmq/scripts/ttl-head-of-line-blocking.py
```

## 接入 RabbitMQ 时的实测差异

这几处是 Boot 4 / Spring AMQP 4 的真实变化，照抄 Boot 3 时代的写法会踩坑：

- **转换器**：Boot 4 使用 Jackson 3（`tools.jackson`），必须用 `JacksonJsonMessageConverter`。旧的 `Jackson2JsonMessageConverter` 依赖 `com.fasterxml.jackson.databind`，而 Boot 4 类路径上只剩 `com.fasterxml.jackson` 的注解包。
- **信任包白名单**：转换器默认只信任 `java.util` / `java.lang`，自定义消息类必须显式 `setTrustedPackages(...)`，否则消费侧抛 `The class ... is not in the trusted packages`。按包名授权即可，不要写成 `"*"`。
- **`CorrelationData.Confirm` 变成了 record**：Spring AMQP 4 起访问器是 `ack()` / `reason()`，旧的 `isAck()` / `getReason()` 已标记待删除。
- **AMQP starter 名未变**：`spring-boot-starter-amqp` 在 Boot 4 里保持原名（不像 `web` → `webmvc`），版本由 Boot BOM 管理（Spring AMQP 4.1.1 + amqp-client 5.30.0）。

## 关于 Spring Boot 4 的几点说明

Spring Boot 4 相对 3.x 是**大版本重构**，本仓库的构建配置按新形式写，与网上大量 Boot 3 教程不同：

- starter 改名：`spring-boot-starter-web` → **`spring-boot-starter-webmvc`**；`aop` → `aspectj`、`json` → `jackson`、`oauth2-client` → `security-oauth2-client` 等，**需逐个对照 BOM，不能套规则**。
- 测试 starter 按技术拆分，如 `spring-boot-starter-webmvc-test`（会传递引入 `spring-boot-starter-test`）。
- 自动配置拆成 47 个模块，包路径整体搬家（`org.springframework.boot.autoconfigure.jms` → `org.springframework.boot.jms.autoconfigure`）。常用注解跟着搬：`@EntityScan` 从 `org.springframework.boot.autoconfigure.domain` 搬到 **`org.springframework.boot.persistence.autoconfigure`**（c02 拆模块时踩过）。
- Jackson 升到 3.0：groupId 为 `tools.jackson`，注解包名仍是 `com.fasterxml.jackson.core`。
- 不再支持 Undertow（未实现 Servlet 6.1），默认 Tomcat 11。
- 测试中 `@MockBean` → `@MockitoBean`。
- 依赖某库不再自动带上其自动配置，需要显式引入对应 starter。
