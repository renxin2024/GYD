#!/usr/bin/env python3
"""
c05 实测组 [1][2]：集群元数据的范围，与队列副本的范围。

对应文章：C05《RabbitMQ 集群、存储与 Quorum Queue》第 2 节起
（「集群成员关系不等于消息副本关系」这条主张的全部物理证据）。

要回答的问题：
  1. 在节点 A 声明 exchange / binding / 两类队列，节点 B、C 能不能查到同一份定义？
  2. classic queue 与 quorum queue 的 leader / members / online 各自是什么？
  3. 「副本」这件事在磁盘上落在哪里？哪些目录出现在几个节点上？

设计要点：
  - 客户端【只连 node1】做全部声明，用 rabbitmqctl 逐个节点查询 —— 这样才能区分
    「定义可见」和「在这一台上可见」。
  - 两类队列用【不同的路由键】，否则一条消息会同时进入两个队列，
    classic 的 messages 计数会翻倍，反而看不清副本范围。
  - 磁盘取证用 du：同一批 200 条 1KB 消息分别投给两类队列，
    看哪个目录在几个节点上出现了、长了多大。

运行（需先起 3 节点集群）：
    cd infra/rabbitmq-cluster
    docker compose up -d
    python3 scripts/scope-of-replication.py

实测结果（RabbitMQ 4.3.5 / Erlang 27.3.4.17，三节点）：
    见 experiments/rabbitmq-cluster/raw-scope-of-replication.log
"""

import json
import os
import shutil
import subprocess
import time

import pika

DOCKER = os.environ.get("DOCKER_BIN") or shutil.which("docker") or "/usr/local/bin/docker"

NODES = [
    ("gyd-rabbitmq1", 5673),
    ("gyd-rabbitmq2", 5674),
    ("gyd-rabbitmq3", 5675),
]
NAME2PORT = dict(NODES)
ALL = [c for c, _ in NODES]
NODE_NAME = {c: "rabbit@" + c[len("gyd-"):] for c, _ in NODES}

EXCHANGE = "gyd.c05.scope.ex"
CLASSIC = "gyd.c05.scope.classic"
QUORUM = "gyd.c05.scope.quorum"
RK_CLASSIC = "scope.classic"
RK_QUORUM = "scope.quorum"

BODY = 1024          # 每条消息正文 1024 字节
N_MSG = 200          # 每类队列各投多少条
VHOST = "/"

CRED = pika.PlainCredentials("admin", "admin123")


# ---------------------------------------------------------------- 基础设施

def log(msg=""):
    print(msg, flush=True)


def sh(*args, timeout=90):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return 124, "", f"命令超时（>{timeout}s）"


def ctl(container, *args, timeout=30):
    """在指定容器上跑 rabbitmqctl —— 拿到的是【该节点自己的】视图。"""
    _, out, err = sh(DOCKER, "exec", container, "rabbitmqctl", *args, timeout=timeout)
    return (out or err).strip()


def diag(container, *args, timeout=30):
    _, out, err = sh(DOCKER, "exec", container, "rabbitmq-diagnostics", *args,
                     timeout=timeout)
    return (out or err).strip()


def in_container(container, cmd, timeout=60):
    _, out, err = sh(DOCKER, "exec", container, "sh", "-c", cmd, timeout=timeout)
    return (out or err).strip()


def connect(port, name=""):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost", port=port, credentials=CRED,
        client_properties={"connection_name": name} if name else None,
        heartbeat=30, blocked_connection_timeout=15,
    ))


# ---------------------------------------------------------------- 拓扑

def drop_topology(port):
    c = connect(port)
    ch = c.channel()
    for q in (CLASSIC, QUORUM):
        try:
            ch.queue_delete(q)
        except Exception:
            pass
    try:
        ch.exchange_delete(EXCHANGE)
    except Exception:
        pass
    c.close()


def declare_topology(port):
    """客户端只连这一个端口，声明全套定义。"""
    c = connect(port, "c05-scope-declarer")
    ch = c.channel()
    ch.exchange_declare(EXCHANGE, "direct", durable=True)
    ch.queue_declare(CLASSIC, durable=True, arguments={"x-queue-type": "classic"})
    ch.queue_declare(QUORUM, durable=True, arguments={
        "x-queue-type": "quorum", "x-quorum-initial-group-size": 3})
    ch.queue_bind(CLASSIC, EXCHANGE, routing_key=RK_CLASSIC)
    ch.queue_bind(QUORUM, EXCHANGE, routing_key=RK_QUORUM)
    c.close()


def publish(port, routing_key, n, label):
    c = connect(port, "c05-scope-publisher")
    ch = c.channel()
    ch.confirm_delivery()
    t0 = time.time()
    for i in range(n):
        body = json.dumps({"label": label, "seq": i, "pad": "x" * (BODY - 64)}).encode()
        ch.basic_publish(EXCHANGE, routing_key, body,
                         properties=pika.BasicProperties(delivery_mode=2))
    dt = time.time() - t0
    c.close()
    return dt


# ---------------------------------------------------------------- 解析

def queue_rows(container):
    """返回 {name: {type,leader,members,online,messages,message_bytes}}"""
    out = ctl(container, "-q", "list_queues", "name", "type", "leader",
              "members", "online", "messages", "message_bytes")
    rows = {}
    for line in out.splitlines():
        if not line.startswith("gyd.c05."):
            continue
        p = (line.split("\t") + [""] * 8)[:8]
        rows[p[0]] = {"type": p[1], "leader": p[2], "members": p[3],
                      "online": p[4], "messages": p[5], "message_bytes": p[6]}
    return rows


def member_list(s):
    return [x.strip() for x in s.strip("[]").split(",") if x.strip()]


# ---------------------------------------------------------------- 主流程

def main():
    log("=" * 78)
    log("c05 实测 [1][2]：元数据的范围 vs 队列副本的范围")
    log(f"环境：RabbitMQ 4.3.5 / 三节点集群 {ALL}")
    log("=" * 78)

    decl_port = NAME2PORT["gyd-rabbitmq1"]

    # =============================== 实验 1 ===============================
    log("\n【实验 1】客户端只连 node1 声明定义，三个节点分别查询")
    drop_topology(decl_port)
    declare_topology(decl_port)
    time.sleep(2)
    log(f"  声明来源：AMQP 客户端 → localhost:5673（即 {NODE_NAME['gyd-rabbitmq1']}）")
    log(f"  exchange = {EXCHANGE}（direct, durable）")
    log(f"  classic  queue = {CLASSIC}  绑定路由键 {RK_CLASSIC}")
    log(f"  quorum   queue = {QUORUM}  绑定路由键 {RK_QUORUM}")

    log("\n  [1a] 在每个节点上执行 rabbitmqctl list_queues，看各自能看到哪些队列")
    seen = {}
    for c in ALL:
        rows = queue_rows(c)
        names = sorted(rows)
        seen[c] = names
        log(f"    {NODE_NAME[c]:<20} 看到 {len(names)} 条队列定义: {names}")

    log("\n  [1b] 每个节点各自看到的 exchange 定义")
    for c in ALL:
        out = ctl(c, "-q", "list_exchanges", "name", "type")
        hit = [l for l in out.splitlines() if l.startswith(EXCHANGE)]
        log(f"    {NODE_NAME[c]:<20} {hit}")

    log("\n  [1c] 每个节点各自看到的 binding")
    for c in ALL:
        out = ctl(c, "-q", "list_bindings", "source_name", "destination_name",
                  "routing_key")
        hit = [l for l in out.splitlines() if "gyd.c05." in l]
        for h in hit:
            log(f"    {NODE_NAME[c]:<20} {h}")

    # 断言：三个节点看到的是同一份定义
    assert len({tuple(v) for v in seen.values()}) == 1, \
        f"三个节点看到的队列定义不一致：{seen}"
    assert all(len(v) == 2 for v in seen.values()), \
        f"每个节点都应看到 2 条队列定义，实际 {seen}"
    log("  ✓ 断言：三个节点看到完全相同的队列定义集合 —— 定义只有一份，全局可见")

    log(f"\n  [1d] 元数据存储自身的 Raft 视图（rabbitmq-diagnostics -q metadata_store_status）")
    out = diag("gyd-rabbitmq1", "-q", "metadata_store_status")
    for line in out.splitlines():
        log("    " + line.replace("|", " ").strip())

    # =============================== 实验 2 ===============================
    log("\n\n【实验 2】两类队列各自的副本范围")
    log(f"\n  [2a] 各投 {N_MSG} 条 {BODY}B 的持久化消息（不同路由键，互不交叉）")
    dt_c = publish(decl_port, RK_CLASSIC, N_MSG, "classic")
    dt_q = publish(decl_port, RK_QUORUM, N_MSG, "quorum")
    log(f"    → classic 路由键 {RK_CLASSIC}：{N_MSG} 条，耗时 {dt_c:.2f}s（已收到 confirm）")
    log(f"    → quorum  路由键 {RK_QUORUM}：{N_MSG} 条，耗时 {dt_q:.2f}s（已收到 confirm）")
    # quorum queue 的 messages / message_bytes 指标是异步刷新的，这里等一下再读
    time.sleep(5)

    log("\n  [2b] 逐节点读取 leader / members / online")
    for c in ALL:
        log(f"    ---- 在 {NODE_NAME[c]} 上查询 ----")
        rows = queue_rows(c)
        for qname in (CLASSIC, QUORUM):
            r = rows.get(qname)
            if not r:
                log(f"      {qname:<26} 该节点看不到")
                continue
            log(f"      {qname}")
            log(f"        type    = {r['type']}")
            log(f"        leader  = {r['leader']}")
            log(f"        members = {r['members']}   （{len(member_list(r['members']))} 个）")
            log(f"        online  = {r['online']!r}")
            log(f"        messages= {r['messages']}   message_bytes = {r['message_bytes']}")

    rows = queue_rows("gyd-rabbitmq1")
    cl, qu = rows[CLASSIC], rows[QUORUM]
    assert len(member_list(cl["members"])) == 1, \
        f"classic queue 的 members 应为 1 个（无内容副本），实际 {cl['members']}"
    assert len(member_list(qu["members"])) == 3, \
        f"quorum queue 的 members 应为 3 个，实际 {qu['members']}"
    log("\n  ✓ 断言：classic members = 1，quorum members = 3 —— 副本范围由队列类型决定")

    log("\n  [2c] 磁盘取证：副本在文件系统上长什么样")
    log("\n    (i) quorum queue 的 Raft 日志目录 —— 每个副本节点一份")
    for c in ALL:
        out = in_container(c, "du -sk /var/lib/rabbitmq/mnesia/rabbit@*/quorum/*/  2>/dev/null")
        log(f"      [{NODE_NAME[c]}]")
        for line in out.splitlines():
            kb, path = line.split("\t") if "\t" in line else (line.split()[0], line.split()[-1])
            log(f"        {kb:>8} KB  ...{path[path.index('/quorum'):]}")

    log("\n    (ii) classic queue 的消息落盘目录 —— 只在属主节点上存在")
    for c in ALL:
        out = in_container(
            c,
            "for d in /var/lib/rabbitmq/mnesia/rabbit@*/msg_stores/vhosts/*/queues/*/; do "
            "  [ -d \"$d\" ] || continue; "
            "  sz=$(du -sk \"$d\" | cut -f1); "
            "  nm=$(sed -n 's/^QUEUE: //p' \"$d/.queue_name\"); "
            "  echo \"$sz KB  $nm  $(basename $d)\"; "
            "done 2>/dev/null")
        log(f"      [{NODE_NAME[c]}]")
        if not out:
            log("        （没有 classic 队列的落盘目录）")
        for line in out.splitlines():
            log(f"        {line}")

    log("\n    (iii) 每个节点 mnesia 目录下的并列子目录")
    for c in ALL:
        out = in_container(c, "ls -1 /var/lib/rabbitmq/mnesia/rabbit@*/ "
                              "2>/dev/null | grep -v plugins-expand")
        log(f"      [{NODE_NAME[c]}] {out.replace(chr(10), ', ')}")

    log("\n    (iv) 每队列 Raft 目录（quorum/<节点>/<前缀_编码名>/）逐个列出")
    for c in ALL:
        out = in_container(
            c,
            "find /var/lib/rabbitmq/mnesia/rabbit@*/quorum -mindepth 2 -maxdepth 2 "
            "-type d 2>/dev/null | sed 's|/var/lib/rabbitmq/mnesia/||'")
        log(f"      [{NODE_NAME[c]}] {out.replace(chr(10), '  |  ') or '（无）'}")

    # 断言：quorum 日志目录在三个节点都出现；classic 消息目录只在一个节点出现
    # quorum 的每队列 Raft 目录 = quorum/<节点名>/<前缀_编码名>/，特征是有 config 文件
    quorum_dirs = 0
    classic_dirs = 0
    for c in ALL:
        q = in_container(c, "find /var/lib/rabbitmq/mnesia/rabbit@*/quorum "
                            "-mindepth 3 -maxdepth 3 -name config -type f 2>/dev/null | wc -l")
        if int(q.strip() or 0) > 0:
            quorum_dirs += 1
        k = in_container(
            c,
            "for d in /var/lib/rabbitmq/mnesia/rabbit@*/msg_stores/vhosts/*/queues/*/; do "
            "  [ -f \"$d/.queue_name\" ] || continue; "
            "  grep -q 'gyd.c05.scope.classic' \"$d/.queue_name\" && echo hit; "
            "done 2>/dev/null | wc -l")
        classic_dirs += int(k.strip() or 0)
    log(f"\n    → 含 quorum queue Raft 日志的节点数 = {quorum_dirs} / 3")
    log(f"    → 含 classic queue 消息目录的节点数 = {classic_dirs} / 3")
    assert quorum_dirs == 3, f"quorum 日志应出现在 3 个节点，实际 {quorum_dirs}"
    assert classic_dirs == 1, f"classic 消息目录应只出现在 1 个节点，实际 {classic_dirs}"
    log("    ✓ 断言：quorum 日志 3 份、classic 消息 1 份 —— 与 members 计数一致")

    # ============================ 小结 ============================
    log("\n" + "=" * 78)
    log("小结")
    log("=" * 78)
    log(f"  元数据（exchange/binding/队列定义）：三个节点看到同一份，1 个 Raft 组")
    log(f"  classic queue 的消息内容：1 个副本，只落在属主节点 {NODE_NAME['gyd-rabbitmq1']}")
    log(f"  quorum queue 的消息内容：3 个副本，{NODE_NAME['gyd-rabbitmq1']} / "
        f"{NODE_NAME['gyd-rabbitmq2']} / {NODE_NAME['gyd-rabbitmq3']} 各一份 Raft 日志")
    log("  → 「集群成员关系」与「消息副本关系」是两个不同的范围")

    log("\n[清理] 保留本次声明的拓扑供后续实验复用；如需清理：")
    log(f"  python3 -c \"import pika;c=pika.BlockingConnection(pika.ConnectionParameters("
        f"'localhost',5673,pika.PlainCredentials('admin','admin123')));ch=c.channel();"
        f"ch.queue_delete('{CLASSIC}');ch.queue_delete('{QUORUM}');"
        f"ch.exchange_delete('{EXCHANGE}')\"")


if __name__ == "__main__":
    main()
