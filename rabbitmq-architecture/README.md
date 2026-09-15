# rabbitmq-architecture —— 前置篇配套代码

对应文章：[前置篇｜RabbitMQ 的架构与特性：一条消息的一生](https://zh.renxinblog.cn/post/gyd-rabbitmq-architecture/)

一条消息从客户端发出到被消费掉，要穿过 vhost、connection、channel、交换机、绑定、队列这几层结构。
这个模块把文章里每一条结论都做成了可单独运行、可复跑的示例：正文只讲机制，实现、运行方式与**完整输出**
放在这里。

## 环境

| 项 | 值 |
|---|---|
| 服务端 | RabbitMQ **4.3.5**（容器名 `gyd-rabbitmq`，管理插件在 `15672`） |
| 客户端 | 裸 `amqp-client` **5.30.0**，没有 `spring-boot-starter-amqp` |
| JDK | 21（`JAVA_HOME` 要显式指向 temurin-21） |
| 构建 | Gradle Wrapper 8.14.2 |

客户端刻意用裸 AMQP 库而不是 Spring AMQP：`RabbitTemplate` 和 `MessageListenerContainer` 恰好把 connection
与 channel 这两层封装掉了，而这两层正是文章要看的对象。

## 运行

```bash
# 1) 起 RabbitMQ
cd infra/rabbitmq && docker compose up -d

# 2) 确认版本
docker exec gyd-rabbitmq rabbitmqctl version          # 期望 4.3.5

# 3) 跑某一个示例（在 GYD 仓根）
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home
./gradlew :rabbitmq-architecture:run --args="<示例名>"
```

几条贯穿全部示例的约定：

- 示例名末尾追加 `keep` 可保留跑完之后的队列与交换机（默认会清理），方便去管理台核对。
- 连接参数可用环境变量覆盖：`RABBITMQ_HOST` / `PORT` / `MGMT_PORT` / `USER` / `PASS` / `VHOST`。
- 所有 AMQP 资源统一用 `gyd.ra.` 前缀，与 c01 的 `gyd.c01.*`、c02 的 `gyd.c02.*` 隔离，互不干扰。
- 队列一律声明为 `durable=true, exclusive=false, autoDelete=false`。本部署（4.3.5）把「既非持久化、
  又非排他」的队列列为**默认拒绝**的弃用特性（`transient_nonexcl_queues = denied_by_default`），
  沿用老教程里的 `durable=false` 会直接撞上 `541`——正是文章第七节那条现象。
- 每个示例跑完会删掉自己建的队列与交换机：队列声明的参数一旦不一致就报 `406`（文章开篇现象 2），
  不清干净会让下一次重跑失败。唯一的例外是第四节的持久化示例——它必须把队列和消息留在那里等一次
  重启，清理放在配套的另一条命令里。

## 示例与文章的对应

9 个示例类共 11 条命令：

| 文章小节 | 示例名 | 这个示例做什么 |
|---|---|---|
| 一 | `topology` | vhost / connection / channel 三层作用域；被拒时 broker 关掉的是 channel 还是 connection；并发共享一条 channel 时 publisher confirm 的序号账本（A / A2 / B / C 四组探针） |
| 二 | `route` | 同一句发布、同一个路由键在 direct / fanout / topic 下的落点对比，含「无绑定 → 落 0 个队列」与「发送端有没有抛异常」 |
| 二 | `default-exchange` | 空串交换机名到底走了什么；能不能对它做显式绑定；管理 API 里读到的那条自动绑定 |
| 三 | `queue-types` | classic / quorum / stream 的声明与消费语义对比；已存在的队列换 `x-queue-type` 会被拒且原队列不变 |
| 四 | `persistence-write` → `persistence-read` | 持久化与复制是两个独立维度：往同一个 durable 队列放 2 条 persistent + 1 条 transient，中间重启 broker，看少的是哪一条 |
| 五 | `consume-ack` | 手动 ack 与 prefetch：ready / 未确认的深度变化；取消消费者与关闭信道的差别；pull 模型对照 |
| 六 | `ttl` | 队列级与单条 TTL 取较小值；队头规则；requeue 不重新计时（配合 `infra/rabbitmq/scripts/ttl-head-of-line-blocking.py`） |
| 六 | `dlx` | 四个死信触发点；`x-death` 的 `reason` 与 `original-expiration` |
| 六 | `max-length` | `x-overflow` 三档的差异（发送端能不能感知）；quorum 上 `reject-publish-dlx` 静默退回 `drop-head` |
| 七 | `precondition` | 声明是幂等比对：`406` 的两个用例与 `403` / `541` / `405` 三个边界用例 |

第四节那一对要按顺序跑，中间那次重启不能省：

```bash
./gradlew :rabbitmq-architecture:run --args="persistence-write"
docker restart gyd-rabbitmq
./gradlew :rabbitmq-architecture:run --args="persistence-read"
```

## 完整输出记录

文章正文只引用关键读数。下面是每个示例的完整输出，按文章小节分组，便于对照复跑。

**一｜连接的协商结果**

```
[A] 已连接 127.0.0.1:5672 vhost=/ user=admin
[A] 服务端自报 version=4.3.5 product=RabbitMQ platform=Erlang/OTP 27.3.4.17
[A] 协商结果 channelMax=2047 frameMax=131072 heartbeat=60
[A] 同一 connection 上开了 3 条 channel：#1 #2 #3
```

**一｜错误的边界停在信道（含新开 #4 继续）**

```
[A2] IOException ⇒ 信道级（soft）｜404 NOT_FOUND - no queue 'gyd.ra.topology.no-such-queue' in vhost '/'
[A2]    之后: connection.isOpen=true, channel.isOpen=false
[A2] 同一条 connection 上新建 #4 并在其上声明 gyd.ra.topology.after-error → 正常
```

**二｜落点表的逐行核对**

```
[route] 发送端抛异常 = false
[route] → 结论：同一句 basicPublish、同一个路由键，落点数量完全由「交换机类型 + 绑定」决定，与队列类型无关
```

**二｜default exchange 的两种被拒手势**

```
[default-exchange] --- 3) 这个内建交换机不许被操作 ---
[default-exchange] IOException ⇒ 信道级（soft）｜403 ACCESS_REFUSED - operation not permitted on the default exchange
[default-exchange] --- 4) 也不许往它上面加绑定 ---
[default-exchange] 队列 gyd.ra.default.q 的绑定: source="" destination=gyd.ra.default.q routing_key="gyd.ra.default.q"
```

**三｜stream 与 classic 的读语义**

```
[type] 第 1 个消费者从 x-stream-offset=first 读到 5 条：[stream-0, stream-1, stream-2, stream-3, stream-4]
[type] 第 2 个消费者同样从 first 读，读到 5 条：[stream-0, stream-1, stream-2, stream-3, stream-4]
[type] classic 第 1 个消费者读到 5 条
[type] classic 第 2 个消费者读到 0 条（期望 0）
```

**四｜重启前后的读数**

```
[persist] 队列 gyd.ra.persist.q ｜ AMQP 实时 ready=3 consumers=0        ← 重启前
[persist] 队列 gyd.ra.persist.q 在重启后仍然存在（durable=true）
[persist] 队列 gyd.ra.persist.q ｜ AMQP 实时 ready=2 consumers=0        ← 重启后
[persist] 重启后读到的消息：[persistent-1, persistent-2]
```

**五｜ack 与 prefetch 的完整读数**

```
[ack] 队列 gyd.ra.ack.q ｜ AMQP 实时 ready=5 consumers=0     ← 刚发布完
[ack] 投递到客户端：deliveryTag=1 body=msg-1（尚未 ack）
[ack] 投递到客户端：deliveryTag=2 body=msg-2（尚未 ack）
[ack] prefetch=2，收到第 2 条后再等 1.5 秒：共投递 2 条
[ack] 队列 gyd.ra.ack.q ｜ AMQP 实时 ready=3 consumers=1     ← 2 条到了客户端手里
[ack] ack 之后共投递 3 条（上限腾出一个名额，第 3 条这才来）
[ack] 队列 gyd.ra.ack.q ｜ AMQP 实时 ready=2 consumers=1
[ack] 队列 gyd.ra.ack.q ｜ AMQP 实时 ready=4 consumers=0     ← 关掉信道
```

**五｜取消消费者之后**

```
[ack] 取消消费者之后 ready=2 → 取消消费者并没有让它们退回 ready
[ack] 再关掉这条信道之后 ready=4
```

**五｜8 个线程共享一条信道**

```
[C3] 8 线程共享 1 条 channel，各读一次 getNextPublishSeqNo() 再发布
[C3] 读到 1600 个序号，其中不同的只有 1572 个 → 重复 28 个（唯一才应该是 1600）
[C3] waitForConfirms=true；确认回调 ack=193 次 nack=0 次（发布共 1600 条）
```

**六｜TTL 的三段读数**

```
[ttl] t=  0.4s  ready=3       （刚发布完）
[ttl] t=  1.9s  ready=3       （还没到期）
[ttl] t=  4.1s  ready=0       （已过 3 秒）
[ttl] t=  5.6s  此时再起消费者 → 读到 0 条（到期消息保证不投递）
[ttl] t=  3.0s  3 秒时深度 = 1   ← 两条消息分别是 1.5 秒与 60 秒，队列级 8 秒压在两者中间
[ttl] t=  9.0s  9 秒时深度 = 0   → 生效的是「队列级与单条里较小的那个」
[ttl] t= 10.3s  深度 = 1         ← TTL 12 秒的消息被另一条连接取走按住 6 秒后退回
[ttl] t= 13.8s  深度 = 0         → requeue 没有重新计时，它按首次发布的时刻到期
```

**六｜死信的四个触发点**

```
[dlx] 1) nack(requeue=false)            → 进死信 1 条  x-death.reason=rejected
[dlx] 2) per-message TTL 到期           → 进死信 1 条  x-death.reason=expired, original-expiration=2000
[dlx] 3) 队列级 x-message-ttl 到期      → 进死信 1 条  x-death.reason=expired
[dlx] 4) 整队被 x-expires 删掉          → 进死信 0 条（符合文档的例外说明）
```

**六｜长度限制的四段实测**

```
[max-length] 1) drop-head：依次发了 drop-1..drop-5，队列深度 = 3
[max-length]    取出的第 1 条 = drop-3  第 2 条 = drop-4  第 3 条 = drop-5
[max-length] 2) reject-publish：waitForConfirms=false；确认回调 ack=3 次 nack=2 次
[max-length] 3) reject-publish-dlx：主队列深度 = 3，死信队列深度 = 2，body=rjd-4 / rjd-5
[max-length] 4) quorum + reject-publish-dlx：主队列深度 = 3，死信队列深度 = 2，body=qq-1 / qq-2
```

**七｜hard 错误的异步上报**

```
[B3] 发布时带上 immediate=true
[B3] （连接被关，异步上报）连接级（hard）｜540 NOT_IMPLEMENTED - immediate=true
[B3] 没有同步异常；之后 connection.isOpen=false, channel.isOpen=false
[B6] 手工塞一个帧类型非法的帧
[B6] （连接被关，异步上报）连接级（hard）｜501 FRAME_ERROR - type 99, all octets = <<1,2,3>>: unknown_frame
```

**七｜406 的两个用例**

```
[pre] 1) 同一个队列加一个 x-message-ttl=1000 再声明一次
[pre]    IOException ⇒ 信道级（soft）｜406 PRECONDITION_FAILED - inequivalent arg 'x-message-ttl' for queue 'gyd.ra.pre.arg' in vhost '/': received the value '1000' of type 'signedint' but current is none
[pre] 2) 类型不一致（classic → quorum）
[pre]    IOException ⇒ 信道级（soft）｜406 PRECONDITION_FAILED - inequivalent arg 'x-queue-type' for queue 'gyd.ra.pre.type' in vhost '/': received 'quorum' but current is 'classic'
```

**七｜被拒之后原队列未变**

```
[type] 把已存在的 classic 队列重新声明成 quorum
[type] IOException ⇒ 信道级（soft）｜406 PRECONDITION_FAILED - inequivalent arg 'x-queue-type' ... received 'quorum' but current is 'classic'
[type] 被拒之后，原队列有没有被改掉？
[type] 队列 gyd.ra.type.classic ｜ type=classic
```

**七｜4.3.0 的三个边界用例**

```
[pre] 3) 队列名用保留前缀 amq.
[pre]    IOException ⇒ 信道级（soft）｜403 ACCESS_REFUSED - queue name 'amq.gyd.ra.pre.reserved' contains reserved prefix 'amq.*'
[pre] 4) durable=false + exclusive=false
[pre]    IOException ⇒ 连接级（hard）｜541 INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated.
[pre] 5) 用另一条 connection 访问独占队列
[pre]    IOException ⇒ 信道级（soft）｜405 RESOURCE_LOCKED - cannot obtain exclusive access to locked queue
```
