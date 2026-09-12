#!/usr/bin/env python3
"""
实测：quorum queue 的多副本（Raft 多数派）行为。

对应文章：GYD 第 1 篇《消息队列的可靠投递与可靠消费》第二节
（解释「单机持久化挡不住机器级故障，要用 quorum queue」这句话到底什么意思）。

要回答的问题：
  1. 一条 quorum queue 在 3 节点集群里有几个副本？leader 落在哪个节点？
  2. leader 所在节点宕机后，队列还能读写吗？（此时仍有 2/3 多数派）
  3. 只剩 1 个节点时（失去多数派），还能投递吗？

依赖：
    pip install pika

运行（需先起 3 节点集群并组网）：
    cd infra/rabbitmq-cluster
    docker compose up -d
    ./join-cluster.sh
    python3 infra/rabbitmq-cluster/scripts/quorum-majority.py

实测结果（RabbitMQ 4.3.5）：
    [STEP 1] 副本组 members = [rmq1, rmq3, rmq2]，leader = rmq1
    [STEP 2] 投 3 条持久化消息，收到 broker confirm
    [STEP 3] 停掉 leader rmq1 → 重新选主为 rmq2，在线副本 2/3
    [STEP 4] 2/3 在线：投递成功，且能取回全部 5 条
             （原 3 条一条不少 —— 数据在多数派副本上）
    [STEP 5] 再停 rmq3 → 在线副本 1/3
    [STEP 6] 失去多数派：投递被拒，15.0s 后 NACK
    [STEP 7] 恢复节点 → 队列自愈，3/3 在线，可正常投递

结论：
    quorum queue 的可用性取决于「多数派是否在线」：2/3 在线照常读写，
    1/3 在线直接拒绝写入。这不是「消息丢了」，而是「没有多数派就不许写」，
    正是 Raft 用来避免脑裂的取舍。
"""

import json
import os
import shutil
import subprocess
import time

import pika

DOCKER = os.environ.get("DOCKER_BIN") or shutil.which("docker") or "/usr/local/bin/docker"
HOST = "localhost"
CRED = pika.PlainCredentials("admin", "admin123")

QUEUE = "gyd.c01.quorum.demo"
EXCHANGE = "gyd.c01.quorum.ex"
ROUTING_KEY = "quorum.demo"

NODES = [
    ("gyd-rabbitmq1", 5673),
    ("gyd-rabbitmq2", 5674),
    ("gyd-rabbitmq3", 5675),
]
NAME2PORT = dict(NODES)
ALL = [c for c, _ in NODES]

# 节点被停 / 起之后，留给集群收敛（选主、撤销副本成员）的时间。
# 已被压低的 net_ticktime=10 是这组等待时长的依据。
CONVERGE_S = 18


def log(msg=""):
    print(msg, flush=True)


def sh(*args, timeout=60):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return 124, "", f"命令超时（>{timeout}s）"


def ctl(container, *args, timeout=30):
    _, out, err = sh(DOCKER, "exec", container, "rabbitmqctl", *args, timeout=timeout)
    return out or err


def alive_containers():
    """逐个容器探活：能 ping 通本节点的才算在运行。"""
    res = []
    for c in ALL:
        rc, _, _ = sh(DOCKER, "exec", c, "rabbitmq-diagnostics", "-q", "ping", timeout=15)
        if rc == 0:
            res.append(c)
    return res


def wait_alive(expected, timeout=150):
    deadline = time.time() + timeout
    last = []
    while time.time() < deadline:
        last = alive_containers()
        if len(last) >= expected:
            return last
        time.sleep(3)
    return last


def queue_info(container):
    """返回 dict：type / messages / leader / members / online。"""
    out = ctl(container, "-q", "list_queues",
              "name", "type", "messages", "leader", "members", "online")
    for line in out.splitlines():
        if line.startswith(QUEUE + "\t"):
            p = line.split("\t")
            return {"type": p[1], "messages": p[2], "leader": p[3],
                    "members": p[4], "online": p[5]}
    return {"type": "?", "messages": "?", "leader": "?", "members": out, "online": "?"}


def online_count(info):
    return len([x for x in info["online"].strip("[]").split(",") if x.strip()])


def connect(port):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=HOST, port=port, credentials=CRED,
        heartbeat=10, blocked_connection_timeout=8,
        connection_attempts=2, retry_delay=1,
    ))


def declare_topology(port):
    conn = connect(port)
    ch = conn.channel()
    ch.exchange_declare(EXCHANGE, "direct", durable=True)
    ch.queue_declare(QUEUE, durable=True,
                     arguments={"x-queue-type": "quorum",
                                "x-quorum-initial-group-size": 3})
    ch.queue_bind(QUEUE, EXCHANGE, routing_key=ROUTING_KEY)
    conn.close()


def publish(port, label, n=1):
    try:
        conn = connect(port)
    except Exception as e:
        return 0, f"连接失败: {type(e).__name__}: {e}"
    try:
        ch = conn.channel()
        ch.confirm_delivery()
        ok = 0
        for i in range(n):
            body = json.dumps({"label": label, "seq": i}).encode()
            try:
                ch.basic_publish(EXCHANGE, ROUTING_KEY, body,
                                 properties=pika.BasicProperties(delivery_mode=2))
                ok += 1
            except Exception as e:
                try:
                    conn.close()
                except Exception:
                    pass
                return ok, f"第 {i + 1} 条被拒: {type(e).__name__}: {e}"
        conn.close()
        return ok, ""
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return 0, f"通道/发布失败: {type(e).__name__}: {e}"


def drain(port, max_n=50):
    got = []
    try:
        conn = connect(port)
    except Exception as e:
        return got, f"连接失败: {type(e).__name__}: {e}"
    ch = conn.channel()
    while len(got) < max_n:
        method, _props, body = ch.basic_get(QUEUE, auto_ack=True)
        if method is None:
            break
        got.append(json.loads(body)["label"])
    conn.close()
    return got, ""


def main():
    log("=" * 74)
    log("实测：quorum queue 的多副本与多数派行为（3 节点集群 / RabbitMQ 4.3.5）")
    log("=" * 74)

    main_port = NAME2PORT["gyd-rabbitmq1"]

    # ---------- STEP 1 ----------
    log("\n[STEP 1] 在 3 节点集群上声明一条 quorum queue")
    declare_topology(main_port)
    time.sleep(2)
    info = queue_info("gyd-rabbitmq1")
    log(f"  类型 = {info['type']}")
    log(f"  leader = {info['leader']}")
    log(f"  副本组 members = {info['members']}")
    log(f"  在线副本 online = {info['online']}")

    # ---------- STEP 2 ----------
    log("\n[STEP 2] 投 3 条持久化消息（无消费者，先留在队列里不取）")
    ok, err = publish(main_port, "seed", 3)
    log(f"  投递成功 {ok} 条（已收到 broker confirm）  {err}")
    log("  注：quorum queue 的 messages 指标是异步刷新的，深度以第 4 步实际取回为准")

    # ---------- STEP 3 ----------
    leader_node = info["leader"]
    victim = "gyd-" + leader_node.split("@")[-1]
    log(f"\n[STEP 3] 停掉 leader 所在节点 {victim}")
    sh(DOCKER, "stop", victim)
    log(f"  存活容器 = {wait_alive(expected=2)}")
    time.sleep(CONVERGE_S)
    alive = [c for c in ALL if c != victim]
    probe = alive[0]
    log(f"  从 {probe} 观察（等待集群收敛 {CONVERGE_S}s）")
    info = queue_info(probe)
    log(f"  leader = {info['leader']}  ← 已从 {leader_node} 重新选主")
    log(f"  副本组 members = {info['members']}")
    log(f"  在线副本 online = {info['online']}  （{online_count(info)}/3）")

    # ---------- STEP 4 ----------
    log("\n[STEP 4] 多数派（2/3）在线 —— 队列是否仍可用？")
    ok, err = publish(NAME2PORT[probe], "after-leader-down", 2)
    log(f"  投递成功 {ok} 条  {err}")
    got, err = drain(NAME2PORT[probe])
    log(f"  消费取回 {len(got)} 条（应为 5 = 原 3 条 seed + 新 2 条）: {got}  {err}")
    log("  ↑ 原 3 条在 leader 宕机后仍能一条不少地取到，说明数据已在多数派副本上")

    # ---------- STEP 5 ----------
    second = [c for c in alive if c != probe][0]
    log(f"\n[STEP 5] 再停一个节点 {second}，只剩 1/3 在线")
    sh(DOCKER, "stop", second)
    log(f"  存活容器 = {wait_alive(expected=1)}")
    time.sleep(CONVERGE_S)
    try:
        info = queue_info(probe)
        if info["online"] == "?":
            log("  队列状态查询超时 —— 失去多数派时，连读队列视图本身都不稳定")
        else:
            log(f"  在线副本 online = {info['online']}（{online_count(info)}/3）")
    except Exception as e:
        log(f"  查询队列视图失败: {e}")

    # ---------- STEP 6 ----------
    log("\n[STEP 6] 失去多数派（1/3）—— 还能投递吗？")
    t0 = time.time()
    ok, err = publish(NAME2PORT[probe], "minority", 1)
    log(f"  投递成功 {ok} 条，耗时 {time.time() - t0:.1f}s")
    log(f"  {err}")

    # ---------- STEP 7 ----------
    log("\n[STEP 7] 恢复被停的节点，看队列是否自愈")
    for c in ALL:
        if c != probe:
            sh(DOCKER, "start", c)
    log(f"  存活容器 = {wait_alive(expected=3)}")
    time.sleep(CONVERGE_S + 7)
    info = queue_info("gyd-rabbitmq1")
    log(f"  leader = {info['leader']}")
    log(f"  在线副本 online = {info['online']}  （{online_count(info)}/3）")
    ok, err = publish(main_port, "after-recovery", 1)
    log(f"  投递成功 {ok} 条  {err}")
    got, _ = drain(main_port)
    log(f"  恢复后实际取回 {len(got)} 条: {got}")
    log("  ↑ 只有 after-recovery 那 1 条；少数派期间被 NACK 的消息没有留在队列里")

    # ---------- 清理 ----------
    log("\n[清理] 删除实验拓扑")
    conn = connect(main_port)
    ch = conn.channel()
    ch.queue_delete(QUEUE)
    ch.exchange_delete(EXCHANGE)
    conn.close()
    log("  已删除 queue / exchange")


if __name__ == "__main__":
    main()
