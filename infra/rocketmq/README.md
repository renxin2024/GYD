# RocketMQ 4.9.7 本地练习集群

GYD 系列共享中间件。**默认 3 个容器**，够跑通「NameServer → 主 → 从」这条链，也够验证主从复制。

| 容器 | 角色 | 说明 |
|---|---|---|
| `gyd-rmq-namesrv` | NameServer | 无状态注册中心，broker 都注册到它 |
| `gyd-rmq-broker-a-master` | broker-a / brokerId=0 | 主节点，消息读写都走它 |
| `gyd-rmq-broker-a-slave` | broker-a / brokerId=1 | 从节点，主动从主节点拉 commitlog 做副本 |

```
客户端 ──► namesrv:9876 ──► 拿到 broker 地址 broker-a → 192.168.31.132:10911
                                    │
                            写主节点 ─┴─► 从节点主动来拉（HA 端口 10912）
```

## 用法

```bash
cd infra/rocketmq
./scripts/up.sh          # 起 3 个容器
./scripts/verify-cluster.sh   # 查集群是否真的组起来
./scripts/down.sh        # 停（数据保留在命名卷）
./scripts/down.sh -v     # 停并清空数据
```

想再扩一个 broker 组（观察队列分散到多个 master、消费组跨 broker 重平衡）：

```bash
./scripts/up.sh b        # 追加 broker-b 主从，共 5 个容器
```

单机就行的话，删掉 `docker-compose.yml` 里的 `broker-a-slave` 整个 service 即可。

> 别直接 `docker compose up`。`up.sh` 多做两件必要的事：按当前宿主 IP 渲染 conf，以及把数据卷属主从 root 改成 3000（原因见下）。

### 核对容器数

正常只有 **3 个**（`./scripts/up.sh b` 是 5 个）。看到更多时按下面排除：

```bash
docker ps -a --filter ancestor=apache/rocketmq:4.9.7 --format '{{.Names}}\t{{.Status}}'
```

- **随机名字的常驻容器**（如 `beautiful_knuth`）= 孤儿。中断过的 `docker run` 会留下它——**杀掉的只是 docker 客户端，容器会继续跑**。`docker compose ps` 看不到它，直接 `docker rm -f <名字>`。
- **`verify-cluster.sh` 运行期间多出来的容器**属于正常：里面的 `mqadmin` 查询是 `docker run --rm` 起的短命容器，几秒就退。

## 端口

宿主端口与容器端口一一对应，便于对照：

| 节点 | listenPort | listenPort-2（VIP 通道，默认不用） | listenPort+1（HA） |
|---|---|---|---|
| namesrv | 9876 | — | — |
| broker-a-master | 10911 | 10909 | 10912 |
| broker-a-slave | 10921 | 10919 | 10922 |
| broker-b-master（`up.sh b`） | 10931 | 10929 | 10932 |
| broker-b-slave（`up.sh b`） | 10941 | 10939 | 10942 |

**客户端接入**：`namesrvAddr` 用当前宿主 IP（值存在 `.host-ip`，本机是 `192.168.31.132:9876`）。

## 三个坑（都实测过）

> 第 2 条的结论在 2026-09-15 凌晨被对照实验**改写**过一次（原说法「默认 true、必须映射 10909」不成立）。

### 1. 必须手改 JVM 参数，否则 6 个节点的默认堆会互相挤爆

`runserver.sh` / `runbroker.sh` 会按「物理内存的一半」自动推堆（上限 8g）。本机 Docker VM 有 11.7G，推出来就是 `-Xmx5g`，broker 还额外把 `-XX:MaxDirectMemorySize` 也设成 5g。

`JAVA_OPT_EXT` 会被追加到默认参数**之后**，同名参数后者生效，用它压到最小：

```
namesrv  -Xms256m -Xmx256m -Xmn128m
broker   -Xms384m -Xmx384m -Xmn192m -XX:MaxDirectMemorySize=256m
```

### 2. 客户端有一个「端口减 2」的开关，默认是**关的**

`MixAll.brokerVIPChannel(isChange, addr)` 的字节码就是 `iconst_2 / isub`——第一个参数为真时把端口减 2，为假时原样返回，**不做任何「是不是本机」的判断**。

关键在于这个 `isChange` 从哪来。4.9.7 里它是**一个字段**：

- `ClientConfig.vipChannelEnabled`，构造器里初始化为
  `Boolean.parseBoolean(System.getProperty("com.rocketmq.sendMessageWithVIPChannel", "false"))`
  ——**缺省 `false`**；
- `DefaultMQProducer.isSendMessageWithVIPChannel()` 的方法体只有一句
  `return isVipChannelEnabled();`（`setSendMessageWithVIPChannel` 同理转发到
  `setVipChannelEnabled`）——**名字不同，同一个字段**。

这个字段有两处使用，覆盖的路径并不一样：

| 路径 | 传给 `brokerVIPChannel` 的是什么 | 会不会去 `listenPort-2` |
|---|---|---|
| 发送（`DefaultMQProducerImpl.sendKernelImpl`） | `producer.isSendMessageWithVIPChannel()` | 开关打开时**才会** |
| 心跳（`MQClientAPIImpl.sendHearbeat`） | 压根没有变换 | **永不**，always `listenPort` |
| 运维/管理类 API（`MQClientAPIImpl` 里 48 个方法） | `clientConfig.isVipChannelEnabled()` | 开关打开时才会 |

所以实测结果（脚本 `experiments/rocketmq/run_vip_probe.sh` 可复跑，原始输出在 `vip-channel-run.log`）：

- **默认配置：客户端只连 `10911`；`10909` 不映射也照样发得出去。**
- 显式 `producer.setVipChannelEnabled(true)`：发送时**另开一条**到 `10909` 的连接（心跳仍然在 `10911` 上）；此时若宿主没映射 `10909`，发送直接失败：

  ```
  RemotingConnectException: connect to 192.168.31.132:10909 failed
  ```

本集群把 `10909` 一起映射，是为了覆盖「有人打开 VIP 开关」这条路径，**它不是默认必需项**——这与「客户端默认就会减 2，所以必须映射 10909」这类常见说法相反。

> **怎么判断客户端连了哪个端口**：不要只看容器里的 ESTABLISHED。
> Docker Desktop 的端口转发套接字在客户端退出后仍会挂着，来源也一律显示成网关
> `192.168.65.1`，看不出是谁连的。要按 PID 归属：
> `lsof -a -nP -p <客户端pid> -iTCP -sTCP:ESTABLISHED`
> （**别漏 `-a`**：`lsof -p PID -iTCP` 是 OR，会把全机连接都列出来。）

### 3. 数据卷属主是 root，而镜像以 uid 3000 运行

Docker 新建命名卷时属主是 `root:root`，镜像里 rocketmq 跑在 `uid=3000`。直接挂上去：

- 写不了 `/home/rocketmq/store` → 存储初始化失败 → `BrokerController.initialize()` 返回 false → `System.exit(-3)`（shell 里看到的退出码是 **253**）
- 写不了 `/home/rocketmq/logs` → 日志文件一个不落，`docker logs` 全空，看起来像「没报错就退出了」

`up.sh` 因此在 `compose create` 之后、`compose up` 之前先把卷属主改成 `3000:3000`。

## 为什么 brokerIP1 写的是宿主局域网 IP

`brokerIP1` 是 broker 对外**通告**地址，客户端拿到它之后会回连。容器名只在容器网络内可解析，宿主机解析不了（实测 `ping`/`curl` 容器 IP 都不通），所以只能写宿主机自己能到达的地址。

用宿主 LAN IP 的好处是**同一个地址两边都通**：

- 宿主上的客户端 → `192.168.31.132:10911` → 命中端口映射 → 进容器 ✅
- 从节点做 HA 复制 → 同样走这个地址 → 从节点经宿主回连主节点 ✅（实测可通）

代价是 IP 变了要重跑 `./scripts/up.sh`（`render-conf.sh` 每次都会重新探测默认路由网卡的 IP）。conf 因此是模板 + 渲染，生成的 `conf/*.conf` 和 `.host-ip` 都不入库。

## 镜像为什么只有 amd64

`apache/rocketmq:4.9.7` 是**单平台 manifest**，没有 arm64；`5.3.3` 才有（实测镜像源上的清单）。

Java 字节码确实跨平台，但镜像不是只有 jar：里面的 **JDK 运行时**（openjdk 1.8.0_372）和 RocketMQ 附带的 **`librocketmq.so`**（走 JNI 的本地库）都是按架构编译的原生程序。4.x 时代缺少 arm64 的 JDK 8 基础镜像，官方就没出 arm64。

本机靠 Rosetta 跑 amd64，实测容器 + JVM 启动约 0.4s，练习够用。

## 常用实验怎么改

改 `conf/*.conf.tmpl`（改模板，别改渲染产物），然后 `./scripts/up.sh`：

| 想验证 | 改什么 |
|---|---|
| 主从同步双写（主节点等从节点确认才返回） | 已经是 `brokerRole = SYNC_MASTER`；对比异步改 `ASYNC_MASTER` |
| 每条消息都等落盘（慢，但断电不丢） | `flushDiskType = SYNC_FLUSH` |
| 从节点承接读流量 | master 上加 `slaveReadEnable = true` |
| 观察从节点落后多少 | 停掉主节点，对比两侧 `mqadmin brokerStatus` 的 `commitLogMaxOffset` |

验证复制是否生效，最直接的是比对主从偏移：

```bash
IMG=apache/rocketmq:4.9.7
IP=$(cat .host-ip)
docker run --rm $IMG sh mqadmin sendMessage -t 你的topic -p hello -n $IP:9876
docker run --rm $IMG sh mqadmin brokerStatus -b $IP:10911 | grep commitLogMaxOffset
docker run --rm $IMG sh mqadmin brokerStatus -b $IP:10921 | grep commitLogMaxOffset
```

实测一条消息后两侧都是 `192`，一致。

## 已知限制

- 切换 `up.sh b` 之后想彻底清掉 broker-b，`docker compose down` 删不掉它（down 只处理当前文件里启用的服务），要 `docker compose --profile b down`，或直接 `docker rm -f gyd-rmq-broker-b-*`。
- 4.x 没有自动主从切换，主节点挂了从节点不会自动升主（要自动化得上 DLedger 或 5.x 的 Controller）。这里只演示复制方向，不演示故障转移。
- 账号密码/ACL 全关，`autoCreateTopicEnable = true`，都是本地练习配置，不要照搬。

## 两个实测出来的坑（跑 demo 时踩到的）

### 删主题不会清消费位点

消费位点存在 broker 上，按 `topic@group` 为键。**删掉并重建同名主题后，位点还在原处**，消费者会按旧位点继续拉——如果新主题的消息数比旧位点小，现象就是「一条都收不到」，而且不报任何错。

实测：`gyd-c03-order` 重建后（6 条消息，offset 0–5），消费组的 `consumerOffset` 仍是 `6`，一次消费收到 **0 条**。

重置：

```bash
docker run --rm apache/rocketmq:4.9.7 sh mqadmin resetOffsetByTime \
  -g <consumerGroup> -t <topic> -s "1970-01-01#00:00:00:000" -f true -n $(cat .host-ip):9876
```

### SQL92 属性过滤需要服务端开关

`enablePropertyFilter` 默认 `false`，此时客户端用 `MessageSelector.bySql(...)` 订阅会**直接报错**：

```
MQClientException: CODE: 1  DESC: The broker does not support consumer to filter message by SQL92
```

它是**服务端**开关（客户端改什么都没用），改完必须重启 broker。本仓库默认已在 `conf/broker-a-master.conf.tmpl` 里打开——只按 tag 过滤不需要它，tag 的 hashcode 就存在 ConsumeQueue 的 20 字节条目里。

顺带一个反直觉的实测结果：`enablePropertyFilter` 打开后重启 broker，第一次发送可能撞上 `RemotingTooMuchRequestException: sendDefaultImpl call timeout`（客户端 3s 超时），再跑一次即正常——重启后的短暂状态，不是配置错。

