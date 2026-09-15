# C03 — 跨行清算对账

GYD 第 3 篇[《跨行清算对账》](https://zh.renxinblog.cn/post/gyd-c03-reconciliation/)的配套代码。

文章讲了三件事：**一笔跨行转账怎么在三个账本上留下痕迹**、**周期对账怎么发现三方对不上**、**发现差异之后为什么有的能自动补、有的必须等人裁决**。这个 demo 把这三件事都做成了可运行的东西，并且**能把四类差异逐个造出来看**——正常路径下三本账总是平的，不注入故障的话什么差异都看不到。

## 一分钟跑起来

前置：JDK 21、Docker（跑 PostgreSQL 与 RabbitMQ）。

```bash
./start.sh                            # 起基础设施 + 三个服务，约 1 分钟
python3 scripts/recon-experiment.py   # 依次造出四类差异并触发对账
./start.sh --down                     # 收工
```

实验脚本只用 Python 标准库，跑完会打印每个场景的账本明细和对账 Job 的真实日志行。

## 三个服务是什么

三个 Gradle 子模块（**模块边界 = 进程边界**），各自连**独立的数据库**：

| 服务 | 模块 | profile | 端口 | 数据库 | 角色 |
|------|------|---------|------|--------|------|
| 结算服务 | `settlement` | 无 | 18080 | `gyd_c03_settlement` | 扮演央行：产出权威结算记录 + 跑对账 Job |
| 工行 | `bank` | `bank-a` | 18081 | `gyd_c03_bank_a` | 付款行（A 的开户行） |
| 建行 | `bank` | `bank-b` | 18082 | `gyd_c03_bank_b` | 收款行（B 的开户行） |

拆模块不是形式主义，它把「三方各自持账、互相看不见」从口头约定升级成编译期约束：`bank` 的 classpath 里根本没有 `SettlementService`，想跨进程直连对方数据都编译不过。此前单模块用三个 `@Profile` 起三次，边界只存在于注解里——JPA 按包扫描实体不受 profile 约束，结果每个库都把三方的表建全了，所谓「隔离」只是数据没写错而已。现在每个可执行模块的主类用 `@EntityScan` 只认自己那份契约（bank 认 `shared.ledger`，settlement 认 `shared.clearing`），**每个库只建自己该有的表**。

两家银行不各建一个模块：它们是同一角色的两个实例，代码完全相同，profile 只选「这个实例是谁」（银行代码、端口、库名、队列名），不再决定「哪段代码跑」。

三个库彼此隔离，谁都查不到别人的分录——对账只能像生产环境那样走 REST 接口拉数据。这个边界是对账成立的前提，所以 demo 里没有图省事共用一个库。

一笔转账的完整链路：

```
POST /api/settle
   ↓
结算服务：落一条 SETTLED 记录（权威源）
   ↓ RabbitMQ 两条 routing key 分别投递
工行队列 ──→ 工行消费 ──→ 写两条分录      建行队列 ──→ 建行消费 ──→ 写两条分录
   ↓
对账 Job：拉结算记录 + 拉两边分录，以 tradeId 为键三方核对
   ↓
差异分级：自动补记 / 挂起等人工 / 等下一批次
```

两家银行各有独立队列，是为了避免竞争消费——一条消息只能被一个消费者拿到，共用队列的话另一家永远收不到。

## 六个场景

`recon-experiment.py` 按顺序跑六个场景，每个都对应文章里的一个论点：

| # | 场景 | 注入什么 | 对账怎么判 | 结局 | 对应文章 |
|---|------|---------|-----------|------|---------|
| 1 | `baseline` | 不注入 | 三方一致 | 平 | 第一、二节 |
| 2 | `missing` | 清掉建行分录 | `MISSING` 漏记 | **自动补记** + 闭环验收通过 | 第四、五、八节 |
| 3 | `duplicate` | 建行绕过幂等再记一组 | `DUPLICATE` 重复入账 | 挂起，等人工裁决冲哪组 | 第五、七节 |
| 4 | `amount` | 工行记成 ¥1,000 | `AMOUNT_MISMATCH` 金额不符 | 挂起，需回溯原始支付指令 | 第二、五节 |
| 5 | `pending` | 结算记录改回 `PENDING` | `NO_SETTLEMENT` 待查 | 什么都不做 | 第五节 |
| 6 | `adjudicate` | 重复入账 + 人工冲正 | 重新判为平 | 差异消失 | 第六、八节 |

单跑某一个：

```bash
python3 scripts/recon-experiment.py --only duplicate
```

分级的判断依据只有一条：**正确的修正动作能不能由权威源唯一确定**。

- 漏记 + 结算已确认 → 补哪侧、补多少都明确 → 自动做
- 重复入账 → `tradeId` 相同只能说明「多记了」，判断不出哪组该冲 → 挂起
- 金额不符 → 结算记录的金额本身也是参与方上报的，看不出哪边对 → 挂起
- 结算侧无记录 → 权威源缺位，宁可什么都不做也不猜 → 等下一批次

场景 4 最值得看一眼：工行记错金额后，**本行账依然是平的**（借贷各 ¥1,000，试算平衡检查显示「平」）。这是文章第二节的核心论点——借贷平衡发现不了金额记错，只有跨方核对才能发现。

## 实测输出

场景 2（漏记 → 自动补记 → 闭环验收）：

```
  注入故障：建行入账通知丢失，清掉建行已记的分录
  建行（注入后）账本（tradeId=TRD-6C73F10C，共 0 条）：
    （空 —— 本行没有任何分录）

  对账结果：
    [recon] 对账简报: 共 2 笔 / 平 1 笔 / 差异 2 条
    [recon] 补记完成: tradeId=TRD-6C73F10C, 银行=建行, 科目=客户存款, 金额=¥10000.0
    [recon][P0][已自动补记] 漏记 | tradeId=TRD-6C73F10C | CCB 科目=客户存款 | 结算=¥10000.00 | 银行=¥0.00 | 分录=[]
    [recon] 补记完成: tradeId=TRD-6C73F10C, 银行=建行, 科目=存放央行款项, 金额=¥10000.0
    [recon][P0][已自动补记] 漏记 | tradeId=TRD-6C73F10C | CCB 科目=存放央行款项 | 结算=¥10000.00 | 银行=¥0.00 | 分录=[]
    [recon] 闭环通过: 2 条自动处理的差异已重新核对为平

  补记后的建行账本（注意 entryId 的 COMP- 前缀 = 对账补记，不是原始入账）：
    [原始] 贷 客户存款 ¥ 10,000.00  entryId=COMP-TRD-6C73F10C-CCB-CUST
    [原始] 借 存放央行款项 ¥ 10,000.00  entryId=COMP-TRD-6C73F10C-CCB-PBOC
```

场景 6（人工裁决冲正 → 差异消失），两条独立证据：

```
    冲正前建行净分录（客户存款多出 ¥10,000，对不上结算）：客户存款 ¥20,000.00，存放央行款项 ¥20,000.00

  冲正后的建行账本（原始分录一条没删，多了两条 REV- 反向分录）：
    [原始] 贷 客户存款 ¥ 10,000.00  entryId=E-TRD-84B0279F-CCB-CUST
    [原始] 借 存放央行款项 ¥ 10,000.00  entryId=E-TRD-84B0279F-CCB-PBOC
    [原始] 贷 客户存款 ¥ 10,000.00  entryId=DUP-TRD-84B0279F-CCB-CUST-...
    [原始] 借 存放央行款项 ¥ 10,000.00  entryId=DUP-TRD-84B0279F-CCB-PBOC-...
    [冲正] 借 客户存款 ¥ 10,000.00  entryId=REV-DUP-TRD-84B0279F-CCB-CUST-...  reversalOf=DUP-...
    [冲正] 贷 存放央行款项 ¥ 10,000.00  entryId=REV-DUP-TRD-84B0279F-CCB-PBOC-...  reversalOf=DUP-...

    冲正后建行净分录（回到每个科目各 ¥10,000，与结算一致）：客户存款 ¥10,000.00，存放央行款项 ¥10,000.00
    净分录校验：通过（冲正前客户存款 ¥20,000.00 → 冲正后 ¥10,000.00）
    断言通过：本轮对账日志里没有 tradeId=TRD-84B0279F 的任何差异行 → 这笔已判为「平」
```

冲正是**追加式**的：原始分录一条都不删，只追加等额反向分录。所以账本条数从 4 条变 6 条，「数条数」证明不了任何事——必须按**净分录**（尚未被冲正的原始分录）重算，这也是闭环验收的口径。

## 代码地图

按模块组织，模块边界即进程边界：

```
c03-reconciliation/
├── shared/              库模块，不可独立启动（三方共用的词汇 + 契约 + MQ 拓扑）
│   └── cn/renxinblog/gyd/c03/shared/
│       ├── domain/                  领域词汇（纯 Java，无 Spring 注解）
│       │   ├── Account.java         科目 + 借贷方向规则表（文章第二节那张表）
│       │   ├── Movement.java        余额增减（业务事实），与 Direction 分开
│       │   ├── Direction.java       借 / 贷（记账符号）
│       │   ├── Bank.java            银行的四套叫法收拢到一处（profile/code/中文名/库名）
│       │   ├── BookingRule.java     一笔转账在本行该记哪两条分录
│       │   └── UnbalancedException.java  借贷不平异常
│       ├── ledger/
│       │   └── JournalEntry.java    分录实体（entryId 唯一，冲正靠 reversalOf 串起来）
│       │                            —— 银行侧 @EntityScan 只扫这一个包
│       ├── clearing/
│       │   ├── SettlementRecord.java 结算记录（对账的裁判数据）
│       │   └── SettlementStatus.java 结算状态
│       │                            —— 结算侧 @EntityScan 只扫这一个包
│       └── mq/
│           └── MqCommonConfig.java  MQ 拓扑声明（与消费者解耦，三服务幂等声明）
│
├── bank/                可执行模块，同一份代码起两次（bank-a / bank-b）
│   └── cn/renxinblog/gyd/c03/bank/
│       ├── BankApplication.java     入口：@EntityScan 收窄到 shared.ledger
│       ├── LedgerService.java       借贷平衡校验 + 幂等记账 + 两种冲正
│       ├── BankSettlementConsumer.java  消费清算消息 → 幂等记账
│       ├── BankController.java      对账 Job 调用的业务接口（拉分录/补记/冲正）
│       ├── BankFaultController.java 银行侧故障注入（实验脚手架）
│       ├── BankProperties.java      读 profile 里的银行代码/队列名
│       └── JournalEntryRepository.java
│
└── settlement/          可执行模块，结算 + 对账同进程 → :18080
    └── cn/renxinblog/gyd/c03/settlement/
        ├── SettlementApplication.java   入口：@EntityScan 收窄到 shared.clearing
        ├── SettlementService.java       落权威记录 + 广播给两家银行
        ├── SettlementController.java
        ├── SettlementFaultController.java 结算侧故障注入
        ├── SettlementRepository.java
        └── recon/                       对账 Job（与结算同进程，可直读权威源）
            ├── ReconJob.java            对账主流程：匹配 → 分级处理 → 闭环验收
            ├── ReconDiff.java           差异记录（只陈述事实，不含「该做什么」）
            ├── ReconDiffType.java       六类差异及各自的处理动作
            ├── ReconAlert.java          分级告警（P0/P1/P2）
            └── ReconTriggerController.java  手动触发一轮对账（不等定时器）
```

为什么契约（`ledger` / `clearing`）放 shared 而不是各自的业务模块：它们是跨进程边界的东西，发送方和接收方都要依赖同一个定义，放在任一业务模块都会造成反向依赖。`shared` 再按 `ledger` / `clearing` 拆两个子包，是为了让两个可执行模块的 `@EntityScan` 各扫一个、互不越界。

两处刻意的设计分离：

- **`ReconDiff` 只描述「差多少」，`ReconJob.processDiff` 才决定「怎么补」**。这是文章第八节讲的「分离发现和修复」——发现差异是纯函数式的比对，修复动作是带风险的操作，两者混在一起就没法单独演进。
- **业务接口（`BankController`）和故障注入（`BankFaultController`）分开**。前者是对账 Job 真实调用的，后者纯粹是实验脚手架。分开之后读代码不用在业务逻辑里分辨「哪些是设计、哪些是为了跑实验」。

## 故障注入端点

**仅 demo 用，生产环境不会暴露。** 没有这些端点，正常路径下三本账永远平，文章第四到八节一次都触发不了。

| 端点 | 服务 | 造什么差异 |
|------|------|-----------|
| `POST /api/bank/fault/miss` | 银行 | 清掉本行分录 → 单边漏记（`MISSING`） |
| `POST /api/bank/fault/duplicate` | 银行 | 绕过幂等再记一组 → 重复入账（`DUPLICATE`） |
| `POST /api/bank/fault/wrong-amount` | 银行 | 写一组错误金额 → 金额不符（`AMOUNT_MISMATCH`） |
| `POST /api/bank/fault/adjudicate-reverse` | 银行 | 人工裁决冲正指定分录（场景 6 用） |
| `POST /api/settlement/fault/pending` | 结算 | 记录改回 `PENDING` → 待查（`NO_SETTLEMENT`） |
| `POST /api/settlement/fault/no-broadcast` | 结算 | 结算成功但不广播 → 双边漏记 |
| `POST /api/bank/fault/reset`、`POST /api/settlement/fault/reset` | 三方 | 清空账本，实验复位 |

`adjudicate-reverse` 要求传入的分录自身借贷平衡（通常是完整一组：客户存款 + 存放央行款项），否则会被 `LedgerService` 的平衡校验拒绝。另外**别把候选分录全选**——两组一起冲掉净额归零，反而错了，人工必须挑出「多余那组」。

## 手动走一遍

不想跑脚本的话：

```bash
# 1) 发一笔转账（工行 → 建行，10,000 元 = 1000000 分）
curl -X POST localhost:18080/api/settle -H 'Content-Type: application/json' \
     -d '{"fromBank":"ICBC","toBank":"CCB","amountCents":1000000}'
# → {"tradeId":"TRD-XXXXXXXX","status":"SETTLED"}

# 2) 看工行账本记了什么（对应文章第二节那张分录表）
curl -s 'localhost:18081/api/bank/journal/TRD-XXXXXXXX' | python3 -m json.tool

# 3) 制造一笔漏记，再手动触发对账（不等 60 秒定时器）
curl -X POST localhost:18082/api/bank/fault/miss -H 'Content-Type: application/json' \
     -d '{"tradeId":"TRD-XXXXXXXX"}'
curl -X POST localhost:18080/api/recon/run

# 4) 看对账 Job 怎么判的
tail -30 logs/settlement.log | grep '\[recon\]'
```

对账 Job 也有定时触发（默认 60 秒一轮，首次延迟 30 秒），`/api/recon/run` 是同步的——调完立即返回，日志里就是这一轮的完整结果，方便和注入动作对齐时间。

## 常见问题

**三个服务撞同一个端口。** 沙箱/CI 环境可能注入 `SERVER__PORT` 环境变量，Spring 的 relaxed binding 会把它映射成 `server.port`，优先级高于 `application-{profile}.yml`。`start.sh` 已经用命令行参数 `--server.port=XXXX` 显式覆盖（命令行优先级最高），手动起服务时记得也带上。

**连不上 localhost，报 502 或 Connection refused。** 如果环境里设了 `http_proxy`，本地回环请求会被代理劫持。curl 加 `--noproxy '*'`，Python 跑脚本前先 `export no_proxy="localhost,127.0.0.1"`。

**Gradle 报 `IllegalArgumentException: 25.0.3`。** Gradle 8.14.2 不认 JDK 25。`start.sh` 已经强制 `JAVA_HOME` 指向 JDK 21，手动跑 gradlew 时要自己 `export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home`（macOS 路径，Linux 用 `/usr/lib/jvm/java-21-openjdk-amd64`）。

**重复跑实验，差异越积越多。** 对账每轮拉的是「当日全部」数据，不复位的话上一轮注入的差异会一直留在账上，简报里的笔数一路涨。跑全套时脚本开头会自动调三个 `reset` 端点清空账本，每轮从零开始；用 `--only` 单跑时不动数据，方便在自己的账本上复现某一类差异。

**`gradlew test` 失败，报删不掉 `build/test-results/test/binary`。** 沙箱拦了 `file-write-unlink`，不是代码问题。判断依据是 `compileJava` / `compileTestJava` 通过即证明代码正确。

**改了 Java 代码，实验行为没变。** 服务加载的还是旧构建。`start.sh` 每次都会先重新 `bootJar` 再启动，所以直接重跑 `./start.sh` 即可；如果是手动 `java -jar` 起的，得先自己构建：

```bash
./gradlew c03-reconciliation:settlement:bootJar c03-reconciliation:bank:bootJar
```

**`./start.sh --down` 之后进程还在。** 早期版本用 `gradlew bootRun` 起服务，脚本记下的 PID 是 Gradle 客户端的，kill 它停不掉真正在跑的 JVM。现在改成 bootJar + `java -jar`，PID 就是服务进程本身；万一还有残留，按 jar 名找：`pkill -f '(settlement|bank)-1.0.0.jar'`。

**`docker compose up` 报 TLS handshake timeout 拉不到镜像。** 容器其实一直好好跑着——compose 发现运行中容器的镜像 tag 和 compose 文件里写的不一致就会尝试重新拉取，离线/代理环境拉不到 docker.io 就直接失败。`start.sh` 已经先探活，`gyd-postgres` 和 `gyd-rabbitmq` 都在运行就跳过 compose up。手动起基础设施时同理：先 `docker ps` 看一眼。

**IDEA 里模块名和文件夹名对不上。** 不是配置错了，是 IDEA 导入 Gradle 工程的固定命名规则：模块名 = `<rootProject.name>` + `.` + 各级工程名（路径段用点连接），并且**每个 source set 还会单独生成一个模块**加 `.main` / `.test` 后缀。于是文件夹 `c03-reconciliation/bank` 对应的模块叫 `gyd.c03-reconciliation.bank`，另外还多出 `…bank.main`、`…bank.test` 两个磁盘上并不存在的模块。前缀 `gyd.` 是固定的，去不掉。能优化的是这几处：

- 子模块目录不带 `c03-` 前缀——外层已经是 `c03-reconciliation/`，再带一次会让模块名里 `c03` 出现两次（`gyd.c03-reconciliation.c03-bank`）。
- 根工程用 `subprojects{}` 下发 group——根工程自己不写代码、不发包，拿到 group 只会让 IDEA 给根模块起 `cn.renxinblog.gyd.gyd` 这种名字，跟其余 `gyd.*` 形态的模块对不上。
- 不需要 source set 级别模块时，到 **Settings → Build Tools → Gradle** 取消勾选 `Generate IntelliJ IDEA module per Gradle source set`（对应 `.idea/gradle.xml` 的 `resolveModulePerSourceSet=false`），14 个模块会收敛成 6 个。
- 改动目录结构后需要在 IDEA 里做一次 Gradle 同步（Gradle 工具窗口的刷新按钮），否则 IDEA 还记着旧路径。
