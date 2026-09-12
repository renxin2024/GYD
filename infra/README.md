# infra —— 系列共享中间件

这里放 **GYD 系列各篇文章共用的中间件运行环境**，不属于任何单篇文章。

```
infra/
└── rabbitmq/
    └── docker-compose.yml
```

## 为什么不放在 `cXX-<slug>/` 里

`cXX-<slug>/` 是**单篇文章的演示代码**目录，随文章增删；而 RabbitMQ、Redis、Kafka 这类组件是**整个系列反复使用的分布式基础设施**——第 1 篇讲可靠投递要用，后面讲幂等、分布式事务、限流熔断大概率还要用。把它们塞进某一篇的目录里，会带来两个问题：

1. **归属错误**：看起来像是「第 1 篇的配套」，实际是全系列共享；
2. **重复与冲突**：后续文章要用同一个组件时，只能复制一份，端口和容器名都会打架。

所以约定：**共享中间件一律放 `infra/<组件>/`，文章目录只放该篇的演示代码。**

## 现有中间件

| 组件 | 目录 | 端口 | 说明 |
|------|------|------|------|
| RabbitMQ 4.3.5 | [`rabbitmq/`](rabbitmq) | 5672（AMQP）/ 15672（控制台） | 消息队列单节点，日常开发与演示 |
| RabbitMQ 4.3.5 三节点集群 | [`rabbitmq-cluster/`](rabbitmq-cluster) | 5673–5675（AMQP）/ 15673–15675（控制台） | 多副本 / 一致性实验，验证 quorum queue 的多数派行为 |

## 使用方式

各中间件独立管理，按需启动，互不干扰：

```bash
cd infra/rabbitmq
docker compose up -d      # 启动
docker compose down       # 停止（数据保留）
```

容器统一以 `gyd-` 前缀命名（如 `gyd-rabbitmq`），卷同样以 `gyd-` 前缀命名，便于识别和统一清理。

三节点集群比单节点多一步组网，首次搭建（或 `docker compose down -v` 之后）要走一次 `join-cluster.sh`：

```bash
cd infra/rabbitmq-cluster
docker compose up -d      # 起三个节点（此时各自独立，尚未成集群）
./join-cluster.sh         # 让 rabbitmq2 / rabbitmq3 加入 rabbitmq1
```

集群节点的 `net_ticktime` 被压到 10 秒（默认 60），目的是让「停掉一个节点看多数派行为」这类实验不必干等一分钟。这是本地实验专用的激进取值，生产不要照搬。

## 实测脚本

文章里那些「实测」结论，脚本都放在对应中间件目录的 `scripts/` 下，可直接复现：

| 脚本 | 对应环境 | 验证什么 |
|------|---------|---------|
| [`rabbitmq/scripts/ttl-head-of-line-blocking.py`](rabbitmq/scripts/ttl-head-of-line-blocking.py) | 单节点 `rabbitmq` | per-message TTL 的队头阻塞：同队列混用不同 TTL 时，过期时机由队头决定 |
| [`rabbitmq-cluster/scripts/quorum-majority.py`](rabbitmq-cluster/scripts/quorum-majority.py) | `rabbitmq-cluster` | quorum queue 的多数派语义：leader 宕机后重新选主、2/3 可读写、1/3 拒写 |

两个脚本都依赖 `pika`：

```bash
python3 -m venv .venv && .venv/bin/pip install pika
.venv/bin/python infra/rabbitmq/scripts/ttl-head-of-line-blocking.py
.venv/bin/python infra/rabbitmq-cluster/scripts/quorum-majority.py
```

脚本默认用 `admin` / `admin123` 连 localhost，与 compose 里的本地开发凭据一致。`quorum-majority.py` 需要调用 `docker` 停 / 起容器，若 `docker` 不在 PATH 里，可用 `DOCKER_BIN=/usr/local/bin/docker` 指定。

## 新增中间件的约定

1. 每个组件一个子目录：`infra/<组件名>/`；
2. 目录内放 `docker-compose.yml`，`name` 用 `gyd-<组件名>`，容器名用 `gyd-<组件名>`；
3. 镜像固定到具体版本号，不要用 `latest`；
4. 只暴露开发必需的端口，凭据明确标注「仅本地开发」；
5. 同步更新本文件的「现有中间件」表格。
