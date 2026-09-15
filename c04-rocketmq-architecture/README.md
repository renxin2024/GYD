# c04-rocketmq-architecture —— 第四话配套代码

对应文章：[第四话｜RocketMQ 的架构与机制：顺序、定时、事务、过滤是同一份 CommitLog 上的四个投影](https://zh.renxinblog.cn/post/gyd-c04-rocketmq-architecture/)

一条消息从生产到消费，要穿过寻址、发送、落盘与建索引、消费取数这几段路径。这个模块把文章里每一条结论
都做成了可单独运行、可复跑的示例：正文只讲机制，实现、运行方式与**完整输出**放在这里。

## 环境

| 项 | 值 |
|---|---|
| 服务端 | RocketMQ **4.9.7**（集群编排见仓库级 [`infra/rocketmq/`](../infra/rocketmq)：namesrv + broker-a 主/从） |
| 客户端 | `rocketmq-client` **4.9.7**，裸客户端，没有 Spring 封装 |
| JDK | 21（`JAVA_HOME` 要显式指向 temurin-21） |
| 构建 | Gradle Wrapper 8.14.2 |

`apache/rocketmq:4.9.7` 只有 amd64 清单，Apple Silicon 上靠 Rosetta 跑（容器 + JVM 启动约 0.4s）。

## 运行

```bash
# 1) 起集群（3 个容器：namesrv + broker-a master/slave）
cd infra/rocketmq && ./scripts/up.sh

# 2) 建本篇要用的主题（队列数固定 4/4，观察落点分布才有可比性）
IP=$(cat infra/rocketmq/.host-ip)
for T in gyd-c03-send gyd-c03-order gyd-c03-filter gyd-c03-delay gyd-c03-tx gyd-c03-offset; do
  docker run --rm apache/rocketmq:4.9.7 sh mqadmin updateTopic \
    -n $IP:9876 -c gyd-rocketmq-cluster -t $T -r 4 -w 4
done

# 3) 跑某一个示例（在 GYD 仓根）
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home
./gradlew :c04-rocketmq-architecture:run --args="<示例名>"
```

几条贯穿全部示例的约定：

- NameServer 地址优先取环境变量 `RMQ_NAMESRV`，缺省是本机练习集群的地址。**宿主机解析不了容器名、
  也连不通容器 IP**，所以只能用宿主机的局域网 IP（换网络后这个地址会变，重跑 `up.sh` 会刷新）。
- 主题名集中定义在 `DemoRunner` 里，示例不自己拼名字。`gyd-c03-autocreate-probe` 是自动建主题那个
  示例**故意不预建**的主题。
- **主题名与消费组/生产者组名沿用录测时的 `gyd-c03-*` / `gyd_c03_*`**：这些是运行期资源名，本篇从
  第三话顺延到第四话时**刻意不改名**——改名对被测行为零影响，却会让下文每一段实测记录里的名字全部
  对不上（那些数字录于 `gyd-c03-*` 时代）。模块自身的编号（目录、包名、主类、Gradle 工程）已跟到 `c04`。
- 每个示例开头打一行 `===== demo: <名字> =====`，便于从长日志里切段对照。
- 客户端进程留了非守护线程，示例结束会显式 `System.exit(0)`，不然 Gradle 会挂住。
- `enablePropertyFilter` 在 `infra` 的 broker 模板里**刻意留 `false`**：`filter-sql` 第一次跑就该失败，
  用来证明这个开关在服务端。想让它成功，改 `true` 后重启 master。
  `delay` 与 `tx` 两个示例的观察窗口是分钟级，跑完整链路要留够时间。

## 示例与文章的对应

13 个示例名，按文章小节分组：

| 文章小节 | 示例名 | 这个示例做什么 |
|---|---|---|
| 一 | `route` | 拉路由：客户端默认间隔（路由刷新 / 心跳 / 位点持久化各是多少）、broker 通告的地址表、队列数 |
| 一 | `vip` | VIP 通道对照：默认连哪个端口、开关打开后多连哪条、端口不可达时报什么异常。参数形如 `vip [on\|off] [发送前持有秒数] [发送后持有秒数]`，持有期间可以从外部认领这条进程的连接 |
| 二 | `send` | 同步 / 异步 / 单向 / 批量四种发送，打印客户端四个发送默认值与重试白名单，并统计落点分布 |
| 二 | `autocreate` | 发到一个不存在的主题：客户端先抛什么，以及自动建出来的队列数（对照显式建的主题） |
| 二 | `failmode` | 单次发送的 `SendStatus`：配合停 / 起从节点，看状态码变了而调用方拿到的东西没变 |
| 五 | `consume` | 并发消费：默认流控参数、8 条消息实际用到几个消费线程 |
| 五 | `offsetprobe` | 位点窗口探针：`kill -9` 与 `shutdown()` 两种退出方式，对照 broker 上的位点与重启后重收的条数。参数形如 `offsetprobe <produce\|consume> <条数>` |
| 六 | `ordered` | 顺序发送：同一业务键固定到同一队列，检查发送端只做了哪一件事 |
| 六 | `orderly` | 顺序消费：同一队列的处理区间是否重叠（重叠 = 并发，不重叠 = 串行），以及每个队列用到几个线程 |
| 七 | `filter-tag` | tag 过滤：订阅 `TAG-A` 时 `TAG-B` 有没有离开过 broker |
| 七 | `filter-sql` | SQL92 属性过滤：开关关闭时订阅报什么错，打开后收到哪些 |
| 八 | `delay` | 固定级延迟：5 个 delayLevel 的标称延迟 vs 实际收到时刻 |
| 九 | `tx` | 事务消息：commit / rollback / unknow 三条路径 + 回查时的调用轨迹 |

## 本机受控实验（三个需要 docker 层操作的）

这三个不是模块里的示例，改的是容器与环境，`inspect-storage` / `ha-failover` 就是文章正文里那两个括注名。

| 实验 | 怎么做的 | 判据 |
|---|---|---|
| 端口映射对照（`vip`） | 复制一份 `docker-compose.yml` 删掉 `10909` 那行，用 `-f` 指向副本重建 broker（`ports` 无法靠叠加 compose 文件移除），跑完换回正式文件重建 | 四格里只有「开关打开且不映射 10909」失败；同一台机器、同一份代码，唯一变量是映射 |
| 硬杀主节点（`ha-failover`） | `docker kill` master，等 40s，再恢复 | 从节点角色与 `brokerId` 不变、日志无升主记录、期间发送拿不到路由 |
| 磁盘逐字节巡检（`inspect-storage`） | `docker cp` 出 `store/` 下的 commitlog 与 consumequeue，按 20 字节定长条目解析第三字段、按 CommitLog 消息头逐条解析主题 | 第三字段按主题三义；延迟档位落点与差值；多主题混写；半消息的 sysFlag 与属性 |

## 完整输出记录

文章正文只引用关键读数。下面是每个示例与实验的完整输出，按文章小节分组，便于对照复跑。

**一｜客户端默认间隔与 broker 通告的地址表**

```
[route] pollNameServerInterval   = 30000 ms   <- 路由刷新间隔
[route] heartbeatBrokerInterval  = 30000 ms   <- 向 broker 心跳间隔
[route] persistConsumerOffset    = 5000 ms    <- 位点持久化间隔（消费端用）
[route] broker broker-a 地址表 = {0=192.168.31.132:10911, 1=192.168.31.132:10921}
[route] 队列 broker-a read=4 write=4 perm=6
[route] 注意：地址表里是 broker 自己通告的地址（brokerIP1），不是 NameServer 的地址
```

**一｜VIP 通道 2×2 对照（阶段 A：10909 已映射）**

```
[vip] 库默认 vipChannelEnabled = false
[vip] 库默认 sendMessageWithVIPChannel = false（只是 isVipChannelEnabled 的转发，同一字段）
[vip] broker 在 NameServer 注册的地址 = 192.168.31.132:10911
[vip] 套用 brokerVIPChannel(true, ...) 之后 = 192.168.31.132:10909
[vip] 发送结果 = SEND_OK, queue=MessageQueue [topic=gyd-c03-send, brokerName=broker-a, queueId=0]
```

发送前后的进程连接（`lsof -a -nP -p <pid> -iTCP -sTCP:ESTABLISHED`，**`-a` 不能漏**）：

```
---------- [A1-default] 开关默认，发送后 ----------
java  17016 ... TCP 192.168.31.132:54867->192.168.31.132:9876  (ESTABLISHED)
java  17016 ... TCP 192.168.31.132:54868->192.168.31.132:10911 (ESTABLISHED)
---------- [A2-vipon] 开关打开，发送后 ----------
java  17125 ... TCP 192.168.31.132:54985->192.168.31.132:9876  (ESTABLISHED)
java  17125 ... TCP 192.168.31.132:54986->192.168.31.132:10911 (ESTABLISHED)
java  17125 ... TCP 192.168.31.132:55020->192.168.31.132:10909 (ESTABLISHED)   ← 多出来的那条
```

**一｜VIP 通道对照（阶段 B：10909 未映射）**

```
宿主侧 lsof -nP -iTCP:10909 -> 无任何监听

[B1-default] 开关默认 → SEND_OK（只连 10911）
[B2-vipon]   开关打开 → 抛 MQClientException
  Send [1] times, still failed, cost [19]ms, Topic: gyd-c03-send, BrokersSent: [broker-a]
  cause[1] = RemotingConnectException: connect to 192.168.31.132:10909 failed
```

阶段 C 是映射恢复后的复跑：`[C1-default]` 与 `[C2-vipon]` 与阶段 A 结论一致（分别为只连 10911、
多连一条 10909），确认环境已复原、结论可复现。

> 一个作废的探针记在这里：自己写的裸 TCP 探测在本机受限环境里不可信——用 `Socket.connect` 探
> `192.168.31.132:1` / `:22` / `:443` 一律返回「连接成功」，而 `nc` 对同一地址时而拒绝时而成功。
> 那一版探测已从代码里删除，不作为证据；结论只依赖上面两条互相印证的事实（客户端明确报
> `connect to …:10909 failed`，且同机同代码只改映射时能连上）。

**二｜发送默认值、重试白名单与落点分布**

```
[send] 默认值: sendMsgTimeout=3000ms retryTimesWhenSendFailed=2 retryTimesWhenSendAsyncFailed=2
[send] 默认值: retryAnotherBrokerWhenNotStoreOK=false  ← false 表示 SendStatus 非 SEND_OK 也不换 broker 重试
[send] 默认值: defaultTopicQueueNums=4 maxMessageSize=4194304 compressMsgBodyOverHowmuch=4096
[send] 重试白名单 retryResponseCodes（命中才换队列重试）= [1, 14, 16, 17, 204, 205]
[send] 同步落点分布 queueId->条数 = {1=1, 2=1, 3=1}
[send] 异步回调 -> queueId=3 sendStatus=SEND_OK
[send] 单向发送 -> 无返回值（既不保证落盘，也没有重试机会）
[send] 批量 ×3 -> 单条 SendResult queueId=1 sendStatus=SEND_OK
```

**二｜自动建主题：先报错的是客户端**

```
[autocreate] 发送前：拉 gyd-c03-autocreate-probe 的路由
[autocreate]   抛 MQClientException CODE=17 DESC=No topic route info in name server for the topic: gyd-c03-autocreate-probe
[autocreate]   即：主题不存在时不是返回 null，而是抛 TOPIC_NOT_EXIST(17)
[autocreate] 自动建：sendStatus=SEND_OK queueId=2
[autocreate] 自动建出的队列 broker-a read=4 write=4
[autocreate] 对照：用 mqadmin updateTopic -w 4 显式建的 gyd-c03-send 是 write=4
```

**二｜停掉从节点前后，单次发送的 `SendStatus` 与耗时**

```
--- 1) 从节点在线 ---
[failmode] sendStatus=SEND_OK              耗时=86ms  queueId=2
--- 2) 停掉 gyd-rmq-broker-a-slave ---
[failmode] sendStatus=SLAVE_NOT_AVAILABLE  耗时=81ms  queueId=1
--- 3) 恢复从节点 ---
[failmode] sendStatus=SEND_OK              耗时=89ms  queueId=1
```

三次都没有抛异常、也没有触发重试（`retryAnotherBrokerWhenNotStoreOK=false`）。第 2 次连耗时都更短，
说明既没有等待从节点，也没有重试——消息照样写进了主节点的 CommitLog。

**三｜`store/` 的实际布局与文件大小**

```
store/
  abort  checkpoint  commitlog/  config/  consumequeue/  index/  lock
commitlog/00000000000000000000          1073741824 字节（1 GiB 稀疏文件）
consumequeue/gyd-c03-send/0/0000…0000    6000000 字节（= 300000 × 20，反证条目定长）
consumequeue/ 下每个主题一个目录：OFFSET_MOVED_EVENT、RMQ_SYS_TRANS_HALF_TOPIC、
  RMQ_SYS_TRANS_OP_HALF_TOPIC、SCHEDULE_TOPIC_XXXX、gyd-c03-* 各主题
```

消费索引前 60 字节（= 3 条 20 字节条目，大端）：

```
0000000 00 00 00 00 00 00 05 42  00 00 00 c0  00 00 00 00
0000016 04 be 52 ae              00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00
```

**三｜8 字节第三字段按主题三义（磁盘巡检）**

```
RMQ_SYS_TRANS_HALF_TOPIC      queueId=0   第三字段 min=0              max=0               int32 量级
RMQ_SYS_TRANS_OP_HALF_TOPIC   queueId=0   第三字段 min=100            max=100             int32 量级
SCHEDULE_TOPIC_XXXX           queueId=0   第三字段 min=1789398197207  max=1789398197207   时间戳
SCHEDULE_TOPIC_XXXX           queueId=17  第三字段 min=1789405396246  max=1789405396249   时间戳
gyd-c03-send                  queueId=0   第三字段 min=84989          max=79581870        int32 量级
gyd-c03-send                  queueId=1   第三字段 min=79581870       max=79581870        int32 量级
gyd-c03-delay                 queueId=1   第三字段 min=0              max=0               int32 量级
```

判据：普通主题的第三字段全在 int32 量级，延迟队列的全在 1.7×10¹² 量级。`"TAG-A"` 的 hashcode 是
79581870。第三字段在 `RMQ_SYS_TRANS_HALF_TOPIC` 下是 0、在 `RMQ_SYS_TRANS_OP_HALF_TOPIC` 下是 100。

**三｜CommitLog 里的多主题混写**

```
解析出 110 条消息（未识别主题 0 条），解析到偏移 22330
消息 110 条，12 个不同主题，相邻两条主题不同 22 次
首次出现顺序：gyd-smoke-test → gyd-c03-order → gyd-c03-autocreate-probe → gyd-c03-send
  → gyd-c03-filter → OFFSET_MOVED_EVENT → SCHEDULE_TOPIC_XXXX → gyd-c03-delay
  → RMQ_SYS_TRANS_HALF_TOPIC → gyd-c03-tx → RMQ_SYS_TRANS_OP_HALF_TOPIC → gyd-c03-offset
```

**三｜索引落后日志多少**

```
mqadmin brokerStatus → dispatchBehindBytes : 0
broker 日志（定时任务打的，主从都会打）：dispatch behind commit log 0 bytes
```

**四｜硬杀主节点（`ha-failover`）**

```
停机前：namesrv 看到 broker-a BID=0(10911) + BID=1(10921)
        master 自报 brokerRole=SYNC_MASTER brokerId=0
        slave  自报 brokerRole=SLAVE       brokerId=1

docker kill gyd-rmq-broker-a-master → exited，等 40s

停主期间发送：MQClientException: No route info of this topic: gyd-c03-send
停主期间 slave：brokerRole=SLAVE brokerId=1（未被提升）
停主期间 slave 日志扫 become|elect|role|master：无匹配行
此时 namesrv 只剩 BID=1

恢复 master → running/healthy，再发一条：broker-a QID=3 SEND_OK
恢复后主从位点：master commitLogMaxOffset=22804，slave commitLogMaxOffset=22804
```

**五｜消费侧默认流控**

```
[consume] 默认流控: consumeThreadMin=20 consumeThreadMax=20 pullThresholdForQueue=1000 条
                     pullThresholdSizeForQueue=100 MB
[consume] 默认流控: consumeConcurrentlyMaxSpan=2000 pullBatchSize=32
                     consumeMessageBatchMaxSize=1 maxReconsumeTimes=-1
[consume] 位点持久化间隔 persistConsumerOffsetInterval=5000 ms
[consume] 汇总: 收到 8 条, 覆盖队列 [0, 1, 3], 实际用到消费线程 8 个
```

**五｜位点上报窗口：强杀 vs 优雅退出（`offsetprobe`）**

A 组（`kill -9`，距启动不到 5 秒，已消费 4 条）——kill 之后 broker 里的位点：

```
#Topic              #QID  #Broker Offset  #Consumer Offset  #Diff  #LastTime
gyd-c03-offset      0     20              0                 20     N/A
```

同组重启后收到 20 条，offset 序列 `0 1 2 3 4 … 19`（前 4 条又消费了一遍）。

B 组（消费完 20 条后 `shutdown()`，会 flush 位点）：

```
#Topic              #QID  #Broker Offset  #Consumer Offset  #Diff  #LastTime
gyd-c03-offset      0     20              20                0      2026-09-14 16:28:42
```

同组重启后收到 0 条。

**五｜删主题不清位点**

删掉并重建 `gyd-c03-order`（6 条消息，offset 0–5）之后，消费组的 `consumerOffset` 仍是 6，一次消费
收到 **0 条**，不报任何错。用 `resetOffsetByTime` 把位点重置到 1970 之后重跑 `orderly`，收到 6 条。

```bash
docker run --rm apache/rocketmq:4.9.7 sh mqadmin resetOffsetByTime \
  -g <consumerGroup> -t <topic> -s "1970-01-01#00:00:00:000" -f true -n $(cat infra/rocketmq/.host-ip):9876
```

**六｜顺序消费：处理区间不重叠（`orderly`）**

```
[orderly] 开始处理 queueId=2 body=order-0 线程=ConsumeMessageThread_…_orderly_1 时刻=1789398118274
[orderly] 处理完成 queueId=2 body=order-0 时刻=1789398118576  ← 与上一条区间不重叠即为串行
…
[orderly] 汇总: 处理 7 条, 每个队列用到的线程 = {2=[ConsumeMessageThread_…_orderly_1]}
```

同一队列 7 条消息全部落在**同一个线程**上，前后区间首尾相接、不重叠。默认消费线程数是 20，
顺序模式下真正跑着的只有一个。

**七｜tag 过滤：`TAG-B` 从未离开 broker**

```
[filter-tag] 已发送 TAG-A ×3 与 TAG-B ×3
[filter-tag] 订阅 TAG-A 实际收到 3 条: [A-0, A-2, A-1]
[filter-tag] TAG-B 的三条从未离开 broker：过滤发生在投递之前，不是客户端收到后再丢
[filter-tag] 依据：tag 的 hashcode 就存在 ConsumeQueue 的 20 字节定长条目里，只读索引即可判断
```

**七｜SQL92 过滤：开关关闭时直接失败**

```
[filter-sql] 已发送 10 条，用户属性 a=1..10，过滤条件是 a > 5
[filter-sql] 订阅失败: org.apache.rocketmq.client.exception.MQClientException:
             CODE: 1  DESC: The broker does not support consumer to filter message by SQL92
```

把 `enablePropertyFilter` 改成 `true` 并重启 master（`mqadmin getBrokerConfig` 核对生效值为 `true`）
之后重跑，收到 5 条：`[a=10, a=6, a=8, a=7, a=9]`。

> 口径提醒：收件条数会随主题里累计的批次变化（单批 5 条、三批 10 条）。判据写成「收到该主题中满足
> 条件的**全部**消息」，不要写死数字。重启后第一次发送可能撞上
> `RemotingTooMuchRequestException: sendDefaultImpl call timeout`（客户端 3s 超时），再跑一次即正常。

**八｜延迟档位：标称 vs 实测（`delay`）**

```
[delay] 发送 level-1   标称延迟=  1000ms  → 实测延迟=1113ms
[delay] 发送 level-3   标称延迟= 10000ms  → 实测延迟=10085ms
[delay] 发送 level-4   标称延迟= 30000ms  → 实测延迟=30042ms
[delay] 发送 level-19  (不在级表内) SEND_OK → 实测延迟=未收到
[delay] 发送 level-99  (不在级表内) SEND_OK → 实测延迟=未收到
```

「未收到」的观察窗口只有几十秒，而这两个级别实际是 2 小时——落点与差值见下一条。

**八｜磁盘侧：超范围级别落在哪，差值多大**

```
queueId=0   属性DELAY=1   差值=1000 ms    (0.0003 h)
queueId=2   属性DELAY=3   差值=10000 ms   (0.0028 h)
queueId=3   属性DELAY=4   差值=30000 ms   (0.0083 h)
queueId=17  属性DELAY=18  差值=7200000 ms (2.0000 h)
queueId=17  属性DELAY=18  差值=7200000 ms (2.0000 h)
```

17 + 1 = 第 18 级，也正是发送 level-19 / level-99 那两条的落点；差值恒为 7200000ms。
第三字段（投递时间戳）减消息存储时间戳就是每条延迟消息的实际延迟。

**九｜事务消息的三条路径与回查（`tx`）**

```
[tx] 发送 tx-commit    localTransactionState=COMMIT_MESSAGE   sendStatus=SEND_OK
[tx] 发送 tx-rollback  localTransactionState=ROLLBACK_MESSAGE sendStatus=SEND_OK
[tx] 发送 tx-unknown   localTransactionState=UNKNOW           sendStatus=SEND_OK
[tx] 消费者可见 t+2281ms tx-commit
[tx] 发送后 5s，消费者已可见: [tx-commit]
[tx] 此刻 tx-unknown 是半消息，对消费者不可见；tx-rollback 已被丢弃
[tx] 等待 broker 回查（transactionCheckInterval 默认 60000ms）...
[tx] 消费者可见 t+19193ms tx-unknown

===== 事务监听器调用轨迹 =====
[tx] t+2249ms  executeLocalTransaction(tx-commit)   -> COMMIT_MESSAGE
[tx] t+2264ms  executeLocalTransaction(tx-rollback) -> ROLLBACK_MESSAGE
[tx] t+2280ms  executeLocalTransaction(tx-unknown)  -> UNKNOW
[tx] t+19183ms checkLocalTransaction(tx-unknown) 被回查 -> COMMIT_MESSAGE
```

**九｜半消息的索引状态与落盘属性**

```
RMQ_SYS_TRANS_HALF_TOPIC  queueId=0  共 4 条（物理偏移 9764 / 10042 / 10586 / 11135，第三字段全为 0）
RMQ_SYS_TRANS_OP_HALF_TOPIC queueId=0 共 3 条（第三字段全为 100）
半消息落盘属性：REAL_TOPIC=gyd-c03-tx  REAL_QID=n  TRAN_MSG=true  事务位( sysFlag & 12 )=无(0)
commit 之后：原主题 gyd-c03-tx 下才第一次出现条目（偏移 10322 与 11440，事务位 COMMIT(8)）
```

官方 `Design_Trancation.md` 的措辞是「消息并不会转发到该原主题的消息消费队列」——不是「不建索引
条目」。半消息确实建了条目，只是建在内部主题下，回查正是靠内部主题里的这条索引把半消息拉起来的。
