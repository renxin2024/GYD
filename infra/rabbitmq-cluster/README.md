# RabbitMQ 三节点集群（GYD 系列共享中间件）

一个三节点的 RabbitMQ 集群，用来回答「多副本」这一类问题：副本到底复制了什么、
一条消息写进 Quorum Queue 的哪一步才算安全、节点一个个掉下去时**发布与消费**分别会怎样。

配套文章：**GYD 第 5 篇**（主题：RabbitMQ 集群的副本范围与多数派；标题以文章定稿为准）。
文章正文不含命令与终端输出，完整命令、读数与预期观察点都在本文件；
原始日志（含作废轮）留在写作仓 `content-production/experiments/rabbitmq-cluster/`。

> **本文件的定位**：它是文章的证据容器，不是生产部署指南。
> 里面所有激进取值（尤其是把节点失联判定压到 10 秒）都只为让实验跑得快，
> 改动它们会直接改变读数，见「[版本与取值约束](#版本与取值约束)」。

---

## 目录

- [这个环境能回答什么](#这个环境能回答什么)
- [版本与取值约束](#版本与取值约束)
- [起停集群](#起停集群)
- [运行顺序](#运行顺序)
- [七组实验：命令、预期观察点、关键读数](#七组实验命令预期观察点关键读数)
- [读数总表](#读数总表)
- [排错索引](#排错索引)
- [文件清单](#文件清单)

---

## 这个环境能回答什么

| 能回答 | 不能回答 |
|---|---|
| 集群元数据复制到哪、队列内容复制到哪（两者范围不同） | 任何吞吐、延迟、容量结论（三个容器跑在同一台机器上） |
| 客户端接入节点 ≠ 队列 Leader 时，发布为什么仍然成功 | 跨机架 / 跨可用区的网络行为 |
| Publisher Confirm 在三条路径上分别确认到了哪一步 | 磁盘故障、操作系统缓冲丢失这类存储层故障 |
| 接入节点故障、Leader 故障、失去多数派这三类故障的区别 | 生产环境下的参数调优建议 |
| 同一个故障时刻，classic 与 quorum 两种队列的结局差异 | stream 的实现行为（本文只用官方原词对照） |
| 一台机器上「容器起来了」与「副本追平了」的差别 | — |

## 版本与取值约束

| 组件 | 版本 | 说明 |
|---|---|---|
| RabbitMQ | `rabbitmq:4.3.5-management` | 元数据后端是 Khepri；`rabbit_mnesia` 不在运行 |
| Erlang | 27.3.4.17（镜像自带） | — |
| Python | 3.13（宿主机） | 脚本只用标准库 + `pika` |
| pika | `1.4.4` | 实测用的客户端；版本不同不影响读数口径 |
| Docker / Docker Compose | 宿主机安装 | 脚本通过 `docker` 子进程 `kill` / `start` 节点 |

`docker-compose.yml` 里有一处**会改变读数的取值**，必须知道：

```yaml
RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS: "-kernel net_ticktime 10"
```

Erlang 默认要 **60 秒**才能判定对端节点下线，「停一个节点看多数派行为」这类实验等不起，
所以压到 **10 秒**。它直接影响下面这些数字：

- 「失去多数派后首个发布多久拿到 `basic.nack`」——实测 **15.01 秒**
  （`rabbit_fifo_client` 的命令超时 = `net_ticktime + 5`）；
- 「已登记信道被挂住多久」——实测 **37.7 秒**仍未返回；
- 成员变更的 20 秒超时预算（源码常量，不受这里影响，但两者会被放在一起比较）。

**所以：这些秒数不能当成默认配置下的数字引用。**

## 起停集群

```bash
cd infra/rabbitmq-cluster

docker compose up -d          # 三个节点各自启动，此时还没组集群
./join-cluster.sh             # 让 rabbitmq2 / rabbitmq3 加入 rabbitmq1，最后打印 cluster_status

docker compose down           # 停止，数据保留在命名卷里；下次起还是同一个集群
docker compose down -v        # 停止并清空数据；下次需要重新执行 join-cluster.sh
```

端口（刻意与单节点的 `infra/rabbitmq/` 错开，两个环境可以同时跑）：

| 节点 | 容器名 | AMQP | 管理控制台 |
|---|---|---|---|
| rabbitmq1 | `gyd-rabbitmq1` | `localhost:5673` | `http://localhost:15673` |
| rabbitmq2 | `gyd-rabbitmq2` | `localhost:5674` | `http://localhost:15674` |
| rabbitmq3 | `gyd-rabbitmq3` | `localhost:5675` | `http://localhost:15675` |

控制台账号：`admin` / `admin123`（仅本地开发；脚本也用它调管理 API）。

国内拉镜像慢时先走镜像源再打回官方 tag：

```bash
docker pull docker.m.daocloud.io/library/rabbitmq:4.3.5-management
docker tag  docker.m.daocloud.io/library/rabbitmq:4.3.5-management rabbitmq:4.3.5-management
```

## 运行顺序

脚本之间**共享同一个集群**，而且**会杀节点**。请按顺序跑，跑完一条确认三节点都在：

```bash
docker ps --format '{{.Names}}\t{{.Status}}' | grep gyd-rabbitmq
```

| 顺序 | 实验 | 脚本 | 是否会杀节点 | 依赖 |
|---|---|---|---|---|
| 1 | [1][2] 元数据范围 vs 副本范围 | `scope-of-replication.py` | 否 | 集群刚组好、干净 |
| 2 | [3] 非 Leader 接入与提交点 | `non-leader-publish.py` | 否 | 无 |
| 3 | [4][5][6] 三类故障时间线 | `failover-timeline.py` | **是**（自己恢复） | 三节点全部在线 |
| 4 | [7] classic 属主节点故障对照 | `classic-owner-failure.py` | **是**（自己恢复） | 三节点全部在线 |
| 补充 | 阶段 2 耗时归因 | `phase2-timing-probe.py` | **是**（自己恢复） | 只在需要复核 [7] 的耗时构成时跑 |
| 5 | [8] 一条消息命中多个队列 | `fanout-per-queue-confirm.py` | **是**（自己恢复） | 三节点全部在线 |
| 6 | [9] 消费侧在三类故障下的行为 | `quorum-consumption-failure-modes.py` | **是**（自己恢复） | 三节点全部在线；约 4.5 分钟 |

统一跑法（在 `infra/rabbitmq-cluster` 目录下）：

```bash
python3 scripts/<脚本名>.py
```

脚本把全过程打到 stdout，**存证靠重定向**：

```bash
python3 scripts/quorum-consumption-failure-modes.py > /path/to/raw-quorum-consumption.log 2>&1
```

> 别忘了 `2>&1`。脚本用 `stderr` 打告警（例如「confirm 数与队列条数不一致」），
> 只重定向 stdout 会把这些告警丢在终端里。

## 七组实验：命令、预期观察点、关键读数

### [1][2] 集群元数据范围 vs 队列副本范围

```bash
python3 scripts/scope-of-replication.py
```

**做什么**：客户端**只连 node1** 声明一个 exchange、两类队列、两条绑定，然后逐个节点用
`rabbitmqctl` 查询，看「同一份定义」在几台机器上可见；再用 `du` 看两类队列的消息
落在几个节点的磁盘上（各投 200 条 1 KB）。

**预期观察点**（不满足就说明集群没组好）：

1. 三个节点都能查到**同一份**队列定义 —— 定义是集群级的；
2. classic queue：`members = 1`，磁盘上的消息目录**只出现在属主节点**（实测约 252 KB）；
3. quorum queue：`members = 3`，三个节点各有一份 Raft 日志目录（实测各约 552 KB）。

**结论**：集群成员关系 ≠ 消息副本关系。元数据是一份、队列内容是 N 份。

### [3] 客户端接入节点 ≠ 队列 Leader

```bash
python3 scripts/non-leader-publish.py
```

**做什么**：用 `rabbitmq-queues transfer_leadership` 把 Leader **确定性地**摆到指定节点，
再把「Leader 位置 × 客户端接入节点」排列组合，逐个组合发布 100 条；
额外做一条 2 副本队列，让客户端连**连副本都不是**的第三个节点。

**证据落在哪**：每条消息发布前后读 `rabbitmq-queues quorum_status` 的
`Last Log Index`，**增量应等于发布条数 + 2**（+2 是信道登记与注销产生的非消息条目）。

**关键读数（实测）**：

- 9 组排列组合**全部拿到 confirm**，单组耗时 **0.099–0.136 秒**（每批 100 条，逐组：
  0.115 / 0.118 / 0.123 / 0.130 / 0.124 / 0.129 / 0.136 / 0.129 / 0.121）；
- Leader 的 Raft 日志增量形状 `(2,1,1,2,1,0)`，逐组 `Δ102 = 100 + 2`；
- 客户端连在既非 Leader、也非副本的节点上，依然成功（100 条 0.099 秒）。

**结论**：没有代理层。信道自己持有队列类型状态，直接把命令发往 Leader 的 Ra 服务器。

### [4][5][6] 三类故障时间线

```bash
python3 scripts/failover-timeline.py
```

**做什么**：一条 quorum queue，一个持续发布的客户端，依次经历三类故障：
① 接入节点被 SIGKILL（队列 Leader 还活着）② 队列 Leader 被 SIGKILL（多数派仍在）
③ 再杀一个副本（只剩 1/3，失去多数派）。每个阶段都测「故障中」与「恢复后」。

**这个脚本最容易被做错的两处**（脚本里已经处理，读日志时要知道它做了）：

- 客户端**到底连在哪台**必须核实：用管理 API 的 `/api/connections` 的 `node` 字段验证，
  验证不过就中止该阶段 —— 否则「停掉接入节点」可能停的是一台与客户端无关的机器；
- **失去多数派有两种表现**，取决于发布方是新连接还是已登记连接：
  新连接的首个发布要先在队列状态机里登记，这条命令会超时并 NACK；
  已登记连接只做 pipeline，没有同步应答，于是被挂住。两者分开测，
  否则会得出「失去多数派不报错」这种错结论。

**关键读数（实测）**：

| 故障 | 读数 |
|---|---|
| 接入节点 SIGKILL | 连接被强制关闭（`CONNECTION_FORCED (320)`），76 次尝试 75 次异常；改连存活节点后 178 次连续成功；**队列 Leader 全程未受影响** |
| Leader SIGKILL（多数派仍在） | 3206 次发布全部拿到确认、0 异常；**单条最长阻塞 348 ms**，之后恢复 |
| Leader SIGTERM（优雅停止） | 4507 次发布全部确认，单条最长阻塞 **41 ms** |
| 失去多数派 · 已登记信道 | **挂住 37.7 秒**仍未返回 |
| 失去多数派 · 新连接 | **15.01 秒**后以 `basic.nack` 失败 |
| 失去多数派时的 `list_queues` | 该轮失败/超时（元数据存储同为 Raft，也要多数派）—— **见排错索引，这条两轮不一致** |
| 造数据阶段 | 10668 条 confirm，立刻读到队列 10661 条 → **指标滞后，不是丢消息**（连读三次稳定在 10668） |

### [7] classic queue 属主节点故障：同一个动作，两种结局

```bash
python3 scripts/classic-owner-failure.py
```

**做什么**：先把 quorum queue 的 Leader 搬到节点 1，再 SIGKILL 节点 1。
于是**同一个故障时刻**：classic queue 失去它唯一的副本，quorum queue 失去 Leader 但仍有 2/3。
故障中往 classic queue 发布分两条路：A = 故障前已建立、已引用过该队列的**旧信道**；
B = 故障后新建的连接与信道。

**关键读数（实测，完整跑过两轮，逐项一致）**：

| | classic（唯一副本离线） | quorum（Leader 离线，2/3 在线） |
|---|---|---|
| 故障中发布 | 旧信道与新信道各 10 条**全部立即被拒**（`NackError`，0.00–0.01 秒） | 10/10 成功确认，中位 1 ms |
| 故障中消费 | 挂 **67.4 / 68.2 / 68.3 秒**后才以 404 失败 | 0.02 秒正常取到 |
| 管理 API 状态 | 仍报 `state = 'running'`，`node` 指向**已离线**节点 | 正常 |
| 恢复后 | 故障前 confirm 的 **51 条一条不少**（逐条消费验证）；故障中那 20 条一条没进去 | 55 条，`online` 3/3 |

**两处反直觉的读数**（本文最值得写的部分）：

1. **发布立即被拒、消费却要挂 68 秒**：两条 AMQP 方法走了两套不同的队列访问封装
   （发布侧不经过重试；`basic.get` 走 `rabbit_amqqueue:with_or_die` 的 **2000 × 30 ms** 重试预算）；
2. **`NACK` 不等于「消息哪儿都没进去」**——见实验 [8]。

### 补充：阶段 2 耗时归因探针

```bash
python3 scripts/phase2-timing-probe.py
```

**为什么单独做这一步**：实验 [7] 首轮日志里，阶段 2 与阶段 3 之间隔了约 **650 秒**，
而其中真正的发布动作是毫秒级的。当时脚本没有给阶段 2 的各观测动作分别计时，事后无法归因。
不猜，单独测一遍：在同样的「classic 属主离线」状态下，把每个动作各自计时。

**结论**：时间落在 `basic_get` 上 —— 三次采样 67.4 / 68.2 / 68.3 秒，
与源码的 2000 × 30 ms = 60 秒预算同量级（差额未逐轮计时）。
**首轮那个 650 秒的间隔因此不再作为结论使用。**

### [8] 一条消息命中多个队列：confirm 是谁的结论

```bash
python3 scripts/fanout-per-queue-confirm.py
```

**做什么**：一个 fanout exchange 同时绑 classic queue（属主在节点 1）与
quorum queue（Leader 摆在节点 2），然后 SIGKILL 节点 1 —— 同一条消息的两个命中目标
被分成「能接受」与「不能接受」。

**关键读数（实测）**：

| 阶段 | 客户端 confirm | quorum 条数增量 | classic 条数增量 |
|---|---|---|---|
| 基线 | 10 / 10 | +10 | +10 |
| 故障中 | **0 / 10**，10 条 `NackError`（0.00–0.01 秒） | **+10** | 属主离线，不可读 |
| 故障中·对照（只发 quorum） | 5 / 5，中位 1 ms | +5 | — |
| 恢复后 | — | 25 条（消费核对） | 仍只有基线那 10 条 |

**三条结论**：

1. **confirm 是 AND 不是 OR**：quorum 侧收下了，classic 侧拒绝，整条消息就没有 confirm；
2. **`NACK` ≠ 消息哪儿都没进去**：quorum queue 的 +10 与客户端的全 NACK **发生在同一时刻**。
   所以重投前必须先按消息 id 去重，否则会在已接受的队列里写第二遍；
3. **客户端看不出是哪个队列拒绝的**：只有一个结局，没有按队列拆分的失败信息。

### [9] 消费侧在三类故障下的行为

```bash
python3 scripts/quorum-consumption-failure-modes.py    # 约 4.5 分钟
```

**做什么**：一条 quorum queue 预置 3000 条积压，**两个消费者做对照**——
A 连在故障节点本身（也是 Leader 所在地），B 连在存活节点。依次经历
基线与三类故障，最后收口核对守恒。

**这个脚本返工过两轮，原因值得写在这里**：pika 消费者的吞吐**随积压量变化**
（prefetch=20 时约 50 条/秒，限速后约 6–7 条/秒），
所以「预置多少条」都不是关键 —— 基线阶段会把积压吃光，后面三个阶段就没有东西可投，
读数全是 0（那不是「消费停了」，是「没东西可投」）。
两轮作废日志按原状留档（`raw-quorum-consumption-v1-vacuous.log`、`-v2-vacuous.log`）。
**修法是给消费者限速（`basic_qos(prefetch_count=1)`），不是加大预置量。**

**关键读数（实测）**：

| 阶段 | 消费者 A（连故障节点） | 消费者 B（连存活节点） | 最长无投递窗口 |
|---|---|---|---|
| 基线 | 192 条 | 192 条 | — |
| Leader 故障（2/3 在线） | 断链一次后 **346** 条 | **348** 条 | A **0.55 s** / B 0.24 s |
| 失去多数派（1/3） | **0 条**（连接未断、无异常） | **0 条**（连接未断、无异常） | 50.59 s（观察窗全长） |
| 恢复后 | 245 条 | 245 条 | 2.85 s |
| 收口 | 合计 **3001** 条投递 / 3000 条预置，队列剩余 0 | | |

**三条结论**：

1. **Leader 故障对消费几乎无感**（与发布侧量级一致）；唯一要处理的是**连在故障节点上的
   那个消费者** —— 它的连接会被断开（异常原文 `StreamLostError: Transport indicated EOF`），
   改连存活节点后 0.5 秒内恢复投递；
2. **失去多数派时消费「静默停止」**：连接不断、不报错、不重连、管理 API 看起来还正常，
   **只是不再有投递** —— 与发布侧「被挂住 / 明确失败」不同，消费侧连失败信号都没有，
   应用侧只能靠**投递停滞监控**发现；
3. **消费者自行重连会造成至少一次投递**：3001 vs 3000，多出的 1 条正是消费者 A 所在节点
   被 SIGKILL 时手里未 ack 的那条被重投；队列剩余 0，**没有丢失**。

**一处如实标注的不一致**：本轮只剩 1/3 时 `list_queues` **仍然可用**
（读到 `messages=1911`），而实验 [4] 的同一时刻它是失败/超时的。
两轮只在「哪个节点存活」「故障顺序」上不同，**归因未定** —— 所以文章只写「可能失败」，
排错索引里也给的是不确定表述。

## 读数总表

| 实验 | 核心读数 | 日志 |
|---|---|---|
| [1][2] | 三节点同见同一份定义；classic `members=1`（磁盘 1 份、约 252 KB）、quorum `members=3`（各约 552 KB） | `raw-scope-of-replication.log` |
| [3] | 9 组排列组合全部 confirm（单组 0.099–0.136 s，每批 100 条）；Leader 日志增量 = 条数 + 2（Δ102） | `raw-non-leader-publish.log` |
| [4][5][6] | Leader SIGKILL 最长阻塞 348 ms（3206/3206）；SIGTERM 41 ms（4507/4507）；接入节点故障 `CONNECTION_FORCED (320)` 75/76 失败 → 改连后 178/178；失去多数派 37.7 s 挂住 / 15.01 s NACK | `raw-failover-timeline.log` |
| [7] | classic 旧新信道各 10 条全部 0.01 s NACK、恢复后 51 条一条不少；quorum 10/10、恢复后 55 条 online 3/3；消费挂 67.4/68.2/68.3 s → 404 | `raw-classic-owner-failure.log` |
| 补充 | 耗时落在 `basic_get`：67.4 / 68.2 / 68.3 s（源码预算是 2000 × 30 ms） | `raw-phase2-timing.log` |
| [8] | 基线 10/10、两队列各 +10；故障中 0 confirm / 10 NACK 而 quorum 仍 +10；对照 5/5 | `raw-fanout-per-queue.log` |
| [9] | 基线 192/192 → 346/348 → 0/0（无异常）→ 245/245；收口 3001/3000 守恒 | `raw-quorum-consumption.log` |

**跨运行复现**：实验 [7] 完整跑了两轮，逐项一致。
实验 [4][5][6] 的三类故障各只跑一轮（单样本口径）；[8] 只跑一轮，但它内部的对照
是同一时刻的两组读数，互为反证；[9] 跑了三轮，前两轮作废并留档。

## 排错索引

| 现象 | 原因 / 判据 | 处置 |
|---|---|---|
| 读到的队列条数比 confirm 成功的条数少 | 条数指标是**周期性上报**的（quorum queue 在 Ra tick 回调里，默认 5 秒一次），读到的是上一份快照 | **等两次读数一致再取值**；不要据此判定丢消息 |
| 管理 API 查不到刚建立的连接 | `/api/connections` 也是周期性采集的 | 轮询到出现为止；别把「查不到」当成「连接落在别的节点」 |
| 故障期间管理 API 仍报 `state = 'running'`，`node` 指向已离线节点 | `state` 反映的是元数据里的记录，不是可用性 | 用 `quorum_status` / `list_queues` 判断；但它们本身也可能读不到（见下一条） |
| `list_queues` 在失去多数派时**可能**失败或超时 | 元数据存储同为 Raft，也要多数派 —— 但两轮实测不一致（[4] 失败、[9] 仍可用），**归因未定** | 别把一次查询结果当成确定的队列状态判据：它可能失败，也可能读得到 |
| `transfer_leadership` 返回非零 | 目标节点**已经是 Leader** 时返回 `rc=69`，文本 `Error: The target node is already the leader of this queue`；结果头走 stdout、错误走 stderr | 把 `already the leader` 当作「目标状态已满足」；判定文本要 **stdout 与 stderr 两路都读** |
| 消费挂住很久才报 404 | classic 读取路径有 **2000 × 30 ms** 的重试预算 | 按「约 1 分钟」预留超时，别在应用侧设更短的硬超时 |
| 节点重启后立刻读写 | 容器 `healthy` ≠ 副本追平 | 用 `quorum_status` 的 `Last Log Index` 与 `online` 验收 |
| 发布拿到 NACK，以为这条消息哪儿都没进去 | confirm 是所有命中队列的 **AND**；被拒绝的那一个决定结局，别的队列可能已经收下了 | 重投前先按消息 id 去重，或把多队列命中改成显式检查 |
| 消费者「连着但收不到消息」，日志里什么都没有 | 失去多数派时消费是**静默停止**：连接不断、不报错、不重连，只是不再投递 | 用「投递停滞时长」而不是「连接是否存活」做告警 |
| 重启节点后出现少量重复消费 | 消费者所在节点被强杀时，手里未 ack 的消息会被重投，这是 at-least-once 的粒度 | 消费侧要做幂等；不要期望重连后不重复 |
| 某个脚本跑完，后面脚本的读数不对 | 脚本共享同一个集群，且会在集群里留下队列、杀掉又拉回节点 | 每条脚本跑完先 `docker ps` 确认三节点都在，再跑下一条；必要时 `docker compose down -v && ./join-cluster.sh` 重置 |
| 脚本报「未在预期时间内就绪」 | 节点刚被 `docker start`，还没起完 | 等几十秒重跑；脚本的等待上限是 60–90 秒 |

## 文件清单

```
infra/rabbitmq-cluster/
├── docker-compose.yml               # 三节点定义（含 net_ticktime=10 的说明）
├── join-cluster.sh                  # 组集群（首次搭建或 down -v 之后执行）
├── README.md                        # 本文件
└── scripts/
    ├── scope-of-replication.py              # [1][2] 元数据范围 vs 副本范围
    ├── non-leader-publish.py                # [3] 接入节点 ≠ Leader
    ├── failover-timeline.py                 # [4][5][6] 三类故障时间线
    ├── classic-owner-failure.py             # [7] classic 单副本故障对照
    ├── phase2-timing-probe.py               # 阶段 2 耗时归因探针（补充）
    ├── fanout-per-queue-confirm.py          # [8] 多队列命中的 confirm 合成
    ├── quorum-consumption-failure-modes.py  # [9] 消费侧三类故障
    └── quorum-majority.py                   # 第 2 篇用的旧脚本，保留
```

**脚本头部注释是「为什么这么设计」的入口**：每条脚本都写明了它刻意分开处理的干扰项
（客户端落点必须核实、两种失败形态必须分开测、条数与 confirm 必须分开记）。
读数对不上时先读那一段，再怀疑环境。

**源码留档**：文章涉及的 4.3.5 源码片段与台账在写作仓
`content-production/experiments/rabbitmq-cluster/`（`official-src/`、`source-ledger.md`、
`official-docs/verbatim-quotes.md`）。本目录只放可运行的部分。
