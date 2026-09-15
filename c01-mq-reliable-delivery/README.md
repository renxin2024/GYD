# c01-mq-reliable-delivery —— 第一话配套代码

对应文章：[第一话｜消息队列的可靠投递与可靠消费](https://zh.renxinblog.cn/post/gyd-c01-mq-reliable-delivery/)

三段兜底（生产端 confirm 与 return、Broker 持久化、消费端 ack 与 prefetch）、重试链路与幂等键都在这个模块里。
文章只讲机制，配置、端点与完整输出放在这里。

## 环境

| 项 | 值 |
|---|---|
| Broker | RabbitMQ **4.3.5**（单节点容器 `gyd-rabbitmq`，管理插件 15672；3 节点集群见仓库级 `infra/rabbitmq-cluster/`） |
| 应用 | Spring Boot **4.1.1** + Spring AMQP **4.1.1** + amqp-client **5.30.0** |
| JDK | 21（`JAVA_HOME` 要显式指向 temurin-21） |
| 构建 / 端口 | Gradle Wrapper 8.14.2 / 应用端口 8080 |

## 运行

```bash
# 1) 起 RabbitMQ（在 GYD 仓根）
cd infra/rabbitmq && docker compose up -d
docker exec gyd-rabbitmq rabbitmqctl version        # 期望 4.3.5

# 2) 起应用
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home
./gradlew :c01-mq-reliable-delivery:bootRun

# 3) 3 节点集群（只有第二节的多数派实验需要）
cd infra/rabbitmq-cluster && docker compose up -d
```

## 端点

每个端点对应正文里的一个判断：

| 端点（POST） | 做什么 | 正文 |
|---|---|---|
| `/gyd/c01/order?orderId=o-1` | 下一单并投递，同步等 Broker 回执；`orderId` 含 `fail` 走「失败→重试→死信」，含 `slow` 每条多处理 `gyd.c01.slow-processing-ms`（默认 200ms） | 一、三、五 |
| `/gyd/c01/order/unroutable?orderId=order-3001` | 投一条路由键无人绑定的消息，观察 return 与 confirm 同时出现 | 一 |
| `/gyd/c01/order/probe?id=p-1&persistent=true\|false` | 投一条探针消息到无消费者的队列 `gyd.c01.persist.probe`，重启 broker 前后对比数量 | 二 |
| `/gyd/c01/order/burst?prefix=slow&count=50` | 批量投递灌出稳定积压，采样 prefetch 与未确认条数 | 三 |

## 拓扑

| 名称 | 值 |
|---|---|
| 业务交换机 / 队列 / 路由键 | `gyd.c01.order.exchange` / `gyd.c01.order.queue` / `order.created` |
| 重试交换机 / 队列 / 路由键 | `gyd.c01.order.retry.exchange` / `gyd.c01.order.retry.queue` / `order.retry` |
| 重试延迟（队列级 TTL） | `RETRY_TTL_MS = 2000` |
| 最终死信交换机 / 队列 / 路由键 | `gyd.c01.order.dlx` / `gyd.c01.order.dlq` / `order.dead` |
| 持久化探针队列 | `gyd.c01.persist.probe`（不绑交换机、不挂消费者） |

链路：业务队列 --nack(requeue=false)--> 重试交换机 → 重试队列 --TTL 到期--> 业务交换机 → 业务队列；
重试用尽时应用显式投到 `gyd.c01.order.dlx`，再 ack 掉原消息（不能 nack，否则再次进入重试循环）。

## 关键配置（对应正文各节）

| 配置 | 值 | 正文 |
|---|---|---|
| `spring.rabbitmq.publisher-confirm-type` | `correlated` | 一（收 confirm 回执） |
| `spring.rabbitmq.publisher-returns` | `true` | 一（收路由失败的退回） |
| RabbitTemplate 的 `mandatory` | `true` | 一（不可路由时才触发 return） |
| `spring.rabbitmq.listener.simple.acknowledge-mode` | `manual` | 三（业务成功才 ack） |
| `spring.rabbitmq.listener.simple.prefetch` | `10`（可用环境变量覆盖，便于对比实验） | 三 |
| `spring.rabbitmq.listener.simple.retry.enabled` | `false` | 三（重试节奏交给重试队列的 TTL） |

另有几处 Boot 4 / Spring AMQP 4 的写法差异（转换器、信任包、`Confirm` record、`inequivalent arg` 等），
正文第八节列了现象与处理，落点都在 `config/RabbitMqConfig.java`。

## 实测脚本

| 脚本 | 用途 | 正文 |
|---|---|---|
| `infra/rabbitmq/scripts/ttl-head-of-line-blocking.py` | 同一条队列投 3 条不同 `expiration` 的消息，只换入队顺序，观察死信时刻 | 六 |
| `infra/rabbitmq-cluster/scripts/quorum-majority.py` | 3 节点集群上停主、观察选主与多数派拒写，再恢复 | 二 |

## 完整输出记录

文章正文只引用关键读数，下面是各处实验的完整输出与读数。

### 一｜不可路由的消息（端点 `unroutable`）

```
$ curl -X POST 'localhost:8080/gyd/c01/order/unroutable?orderId=order-3001'

[producer] 已投递到无人绑定的路由键: orderId=order-3001
[return] 消息未能路由到队列: exchange=gyd.c01.order.exchange, routingKey=order.nowhere, replyText=NO_ROUTE
[confirm] Broker 已确认消息: id=order-3001
```

查 `gyd.c01.order.queue` 与 `gyd.c01.order.retry.queue` 深度均为 0。

### 二｜重启前后（端点 `probe`）

| 步骤 | `messages` | `messages_persistent` |
|---|---|---|
| 投 1 条持久化 + 1 条非持久化（两条都收到 confirm ack） | 2 | 1 |
| `docker restart gyd-rabbitmq` 之后 | 1 | 1 |

再单独投一条非持久化消息复验：

| 步骤 | `messages` | `messages_persistent` |
|---|---|---|
| 只投 1 条非持久化 | 1 | 0 |
| 重启之后 | 0 | 0 |

### 二｜3 节点集群上的多数派（脚本 `quorum-majority.py`）

| 步骤 | 观察结果 |
|---|---|
| 声明 quorum queue | 副本组 members 实为 3 个，leader = rmq1 |
| 投 3 条持久化消息 | 全部收到 broker confirm |
| 停掉 leader 所在节点 | 重新选主（两次运行分别选到 rmq2 / rmq3），在线副本 2/3 |
| 2/3 在线时再投 2 条 | 投递成功，且取回全部 5 条——原 3 条一条不少 |
| 再停一个（只剩 1/3） | 投递被拒，15.0s 后返回 `NackError: 0 message(s) NACKed` |
| 恢复被停节点 | 队列自愈（3/3），投递恢复；被 NACK 的那条没有留在队列里 |

失去多数派期间连 `rabbitmqctl list_queues` 都会超时，采样会时好时坏。

### 三｜prefetch 与未确认条数（端点 `burst`）

50 条消息，每条固定处理 200ms：

| prefetch | `messages_unacknowledged` | 50 条排空耗时 |
|---|---|---|
| 1 | 恒为 1 | 约 10s |
| 20 | 恒为 20 | 约 10s |

「恒为」只成立在积压充足阶段；排空最后一段队列里不足 prefetch 条时，未确认条数会掉下来。

### 五｜重试链路的完整日志（`orderId=order-fail-retry-2`）

```
[consumer] 处理失败，2000ms 后重试（第 1/3 次）: orderId=order-fail-retry-2, x-death=(无)
[consumer] 处理失败，2000ms 后重试（第 2/3 次）: x-death=gyd.c01.order.retry.queue/expired ×1 + gyd.c01.order.queue/rejected ×1
[consumer] 处理失败，2000ms 后重试（第 3/3 次）: x-death=gyd.c01.order.retry.queue/expired ×2 + gyd.c01.order.queue/rejected ×2
[consumer] 重试 3 次仍失败，转入死信队列: orderId=order-fail-retry-2, 累计投递 4 次
```

相邻两次处理的间隔实测 2004 / 2003 / 2003ms（重跑有几毫秒浮动），与队列上配的 2000ms TTL 对得上。

### 六｜队头阻塞（脚本 `ttl-head-of-line-blocking.py`）

同一条队列投 3 条消息，各带不同的 `expiration`：

| 实验 | 入队顺序 | 实际死信时刻 |
|---|---|---|
| A | 6000ms → 1000ms → 1000ms | 三条全部在 +6.0s |
| B | 1000ms → 1000ms → 6000ms | +1.0s / +1.0s / +6.0s |
