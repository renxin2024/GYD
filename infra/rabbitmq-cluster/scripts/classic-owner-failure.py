#!/usr/bin/env python3
"""
c05 实测组 [7]：classic queue 属主节点故障 —— 「没有副本」与「多数派选主」的差别。

对应文章：C05 第 5 节（两类队列的故障语义对照）与第 3 节（confirm 的精确边界）

要回答的问题：
  1. 同一次故障同时打在两类队列上，可观察结果差在哪？
  2. classic queue 的属主节点消失后，客户端从另一节点往它发消息会发生什么 ——
     被拒绝，还是「拿到了 confirm 但消息没有落到任何地方」？
  3. 故障前就已建立、已经引用过这条队列的同一个信道，表现是否不同？
  4. classic queue 的属主节点回来之后，故障前 confirm 过的消息还在不在？
  5. 故障期间，这条 classic queue 的消息内容在另外两个节点上有没有副本？

设计要点（四处刻意为之）：
  · 【一个动作，两种结果】先把 quorum queue 的 Leader 用
    `rabbitmq-queues transfer_leadership` 摆到节点 1，再 SIGKILL 节点 1。
    于是 classic queue 失去它唯一的副本，quorum queue 失去 Leader 但仍有 2/3。
    同一个故障时刻、同一种故障强度，排除「故障力度不同」这个干扰项。
  · 【confirm 与入队分开核对】故障期间的发布结果按「异常类型原文」留档；
    同时把「拿到 confirm」和「消息真的进了队列」分成两件事 —— 后者由阶段 3
    恢复后的 list_queues 条数决定。两者不一致时以条数为准，脚本会显式告警。
  · 【两条发布路径分开测】故障中向 classic queue 发布分 A/B 两路：
    A = 故障前已建立并已引用过该队列的同一信道，B = 故障后新建的连接与信道。
    只测 B 会漏掉「信道持有旧引用」这类表现差异（quorum queue 在实测 [4] 里
    就因为「已登记 / 新连接」而分成「被挂住」与「被拒绝」两种结果）。
  · 【副本存在性用文件系统判定】故障期间在节点 2/3 上找 classic queue 的消息目录，
    找到 0 个才敢说「唯一副本正在离线」。不拿「查不到队列」当副本不存在的证据。

运行（需先起 3 节点集群）：
    cd infra/rabbitmq-cluster
    docker compose up -d
    python3 scripts/classic-owner-failure.py

实测结果（RabbitMQ 4.3.5，三节点）：见
    experiments/rabbitmq-cluster/raw-classic-owner-failure.log
"""

import base64
import json
import os
import shutil
import subprocess
import time
import urllib.parse
import urllib.request

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

EXCHANGE = "gyd.c05.owner.ex"
CLASSIC = "gyd.c05.owner.classic"
QUORUM = "gyd.c05.owner.quorum"
RK_CLASSIC = "owner.classic"
RK_QUORUM = "owner.quorum"

VICTIM = "gyd-rabbitmq1"        # 同时承载 classic 唯一副本与 quorum Leader
SURVIVOR = "gyd-rabbitmq2"      # 故障期间客户端改连这里

N_BASE = 50                     # 基线各投 50 条
N_PREFAULT = 1                  # 长连接预置的一条（走 classic）
N_OUTAGE = 10                   # 故障中每条路径各试 10 条
BODY = 1024

CRED = pika.PlainCredentials("admin", "admin123")

_t0 = time.time()


def log(msg=""):
    print(msg, flush=True)


def ts():
    return time.time() - _t0


def mark(msg):
    log(f"\n[t+{ts():.2f}s] {msg}")


def sh(*args, timeout=120):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return 124, "", f"命令超时（>{timeout}s）"


def in_container(container, cmd, timeout=60):
    _, out, err = sh(DOCKER, "exec", container, "sh", "-c", cmd, timeout=timeout)
    return (out or err).strip()


# ------------------------------------------------------------------ 观测

def api(path):
    """管理 API。任一节点被停时 15673 就不可达，所以三个端口都要试。"""
    errors = []
    for p in (15673, 15674, 15675):
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{p}/api/{path}")
            req.add_header("Authorization", "Basic " + base64.b64encode(
                f"{CRED.username}:{CRED.password}".encode()).decode())
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            return json.loads(opener.open(req, timeout=10).read())
        except Exception as e:
            errors.append(f"{p}:{type(e).__name__}")
    raise RuntimeError("管理 API 全部不可达 " + ",".join(errors))


def api_queue(qname):
    """管理 API 的队列对象。失败时返回 {'_error': ...}，不抛。"""
    try:
        return api("queues/%2F/" + urllib.parse.quote(qname, safe=""))
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {str(e)[:80]}"}


def client_node(conn_name):
    try:
        for c in api("connections"):
            nm = next((v for k, v in (c.get("client_properties") or {}).items()
                       if k == "connection_name"), None)
            if nm == conn_name:
                return c.get("node")
    except Exception:
        return None
    return None


def wait_client_node(conn_name, ctr_expected, timeout=25):
    """等管理 API 的连接表把这条连接采集进来再判定落点。

    管理 API 的连接表是【周期性采集】的，刚建好的连接立刻查会查不到 ——
    不能把「查不到」当成「落在别处」。返回 (实际节点, 等待秒数)。
    """
    t0 = time.time()
    while time.time() - t0 < timeout:
        n = client_node(conn_name)
        if n == NODE_NAME[ctr_expected]:
            return n, round(time.time() - t0, 1)
        time.sleep(1)
    return client_node(conn_name), round(time.time() - t0, 1)


def quorum_status(queue=QUORUM, timeout=25):
    for ctr in ALL:
        rc, out, err = sh(DOCKER, "exec", ctr, "rabbitmq-queues", "quorum_status",
                          queue, "--formatter", "json", timeout=timeout)
        txt = (out or err).strip()
        i, j = txt.find("["), txt.rfind("]")
        if i < 0:
            continue
        try:
            rows = json.loads(txt[i:j + 1])
        except json.JSONDecodeError:
            continue
        return {r["Node Name"]: {"state": r["Raft State"], "log_index": r["Last Log Index"],
                                 "term": r["Term"]} for r in rows}
    return {}


def queue_row(qname, timeout=25):
    """rabbitmqctl list_queues 的 JSON 视图（任一存活节点）。"""
    for ctr in ALL:
        rc, out, err = sh(DOCKER, "exec", ctr, "rabbitmqctl", "-q", "list_queues",
                          "name", "type", "leader", "members", "online", "messages",
                          "--formatter", "json", timeout=timeout)
        txt = (out or err).strip()
        i, j = txt.find("["), txt.rfind("]")
        if i < 0:
            continue
        try:
            for r in json.loads(txt[i:j + 1]):
                if r.get("name") == qname:
                    return r
        except json.JSONDecodeError:
            continue
    return {}


def ml(v):
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    return [x.strip() for x in str(v or "").strip("[]").split(",") if x.strip()]


def messages_of(qname):
    r = queue_row(qname)
    if not r:
        return None
    try:
        return int(str(r.get("messages", "")).strip() or 0)
    except ValueError:
        return None


def stable_messages(qname, timeout=45, gap=5):
    """等条数指标稳定后再取值。

    队列的 messages 是【周期性采集并发布】的指标，不是读取瞬间的真值：
    刚发布完立刻读会偏小。实测 [4][5][6] 收尾时读到 classic/quorum 少 7 条，
    停一会儿复读就与 confirm 条数完全一致 —— 那是滞后，不是丢消息。
    所以凡是拿条数当判据的地方，都必须等两次读数一致。
    """
    t0, prev, n_reads = time.time(), messages_of(qname), 0
    while time.time() - t0 < timeout:
        time.sleep(gap)
        cur = messages_of(qname)
        n_reads += 1
        if cur is not None and cur == prev:
            return cur, n_reads
        prev = cur
    return prev, n_reads


def alive():
    return [c for c in ALL
            if sh(DOCKER, "exec", c, "rabbitmq-diagnostics", "-q", "ping",
                  timeout=15)[0] == 0]


def wait_alive(n, timeout=180):
    deadline = time.time() + timeout
    last = []
    while time.time() < deadline:
        last = alive()
        if len(last) >= n:
            return last
        time.sleep(3)
    return last


def wait_online(n, timeout=210):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        r = queue_row(QUORUM)
        if r:
            last = len(ml(r.get("online")))
            if last >= n:
                return last
        time.sleep(3)
    return last


def classic_store_dir(container, timeout=45):
    """返回该节点上 classic queue 的消息落盘目录（可能为空）。"""
    return in_container(
        container,
        "for d in /var/lib/rabbitmq/mnesia/rabbit@*/msg_stores/vhosts/*/queues/*/; do "
        "  [ -f \"$d/.queue_name\" ] || continue; "
        f"  grep -q '{CLASSIC}' \"$d/.queue_name\" && du -sk \"$d\" | cut -f1 | "
        "    xargs -I{} echo \"{} KB  $d\"; "
        "done 2>/dev/null", timeout=timeout)


def classic_dir_files(container):
    """落盘目录的原始 ls -la —— 用来对照「SIGKILL 前后是不是同一批文件」。

    直接输出原始行，不经过 awk 重排：重排会在目录被压缩或清空的那一刻
    产出一段空输出，看起来像「读不到」，实际上是把「目录此刻是空的」这条信息
    丢掉了。原始 ls 会老老实实说清楚条目数。
    """
    return in_container(
        container,
        "for d in /var/lib/rabbitmq/mnesia/rabbit@*/msg_stores/vhosts/*/queues/*/; do "
        "  [ -f \"$d/.queue_name\" ] || continue; "
        f"  grep -q '{CLASSIC}' \"$d/.queue_name\" || continue; "
        "  echo \"$(du -sk \"$d\" | cut -f1) KB  $(ls -A \"$d\" | wc -l) 个条目  $d\"; "
        "  ls -la \"$d\"; "
        "done 2>/dev/null")


# ------------------------------------------------------------------ 拓扑

def connect(port, name="", heartbeat=10):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost", port=port, credentials=CRED,
        client_properties={"connection_name": name} if name else None,
        heartbeat=heartbeat, blocked_connection_timeout=25, connection_attempts=1))


def declare_topology():
    c = connect(NAME2PORT[VICTIM], "c05-owner-declarer")
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
    ch.exchange_declare(EXCHANGE, "direct", durable=True)
    ch.queue_declare(CLASSIC, durable=True, arguments={"x-queue-type": "classic"})
    ch.queue_declare(QUORUM, durable=True, arguments={
        "x-queue-type": "quorum", "x-quorum-initial-group-size": 3})
    ch.queue_bind(CLASSIC, EXCHANGE, routing_key=RK_CLASSIC)
    ch.queue_bind(QUORUM, EXCHANGE, routing_key=RK_QUORUM)
    c.close()


def move_quorum_leader_to(node_name):
    """把 quorum queue 的 Leader 摆到指定节点 —— 让两类队列共用一个故障时刻。

    参数是【节点名】（如 rabbit@rabbitmq1），因为 CLI 要的就是节点名。

    两处 4.3.5 CLI 的真实行为（实测确认，不靠猜）：
      · 目标节点是【位置参数】，不是 `--node`（该开关不被 4.3.5 接受）；
      · 目标节点已经是 Leader 时返回 rc=69 并打印
        “Error: The target node is already the leader of this queue”，
        这是目标状态已满足，不是失败，必须当成成功接受。
    """
    target = node_name
    last = ""
    for c in ALL:
        rc, out, err = sh(DOCKER, "exec", c, "rabbitmq-queues", "transfer_leadership",
                          QUORUM, target, timeout=30)
        # CLI 把结果头写到 stdout、把 Error 写到 stderr —— 只看其中一路会漏掉判定文本。
        last = ((out or "") + " " + (err or "")).strip()
        if rc == 0:
            return last.replace("\n", " ")[:110]
        if "already the leader" in last:
            return "目标节点已经是 Leader（rc=69 already the leader，无需搬迁）"
    return f"transfer_leadership 未能确认（{last[:100]}）"


def publish_on_channel(ch, routing_key, n, label):
    """在给定信道上发布，返回 (逐条结果, 实际尝试次数)。

    信道被 broker 关闭后不再继续尝试 —— 后续失败只会是
    ChannelWrongStateError 的回声，混进来会掩盖真正的失败样本。
    """
    recs, attempted = [], 0
    for i in range(n):
        if getattr(ch, "is_closed", False):
            break
        body = json.dumps({"label": label, "seq": i,
                           "pad": "x" * (BODY - 64)}).encode()
        t0 = time.time()
        attempted += 1
        try:
            ch.basic_publish(EXCHANGE, routing_key, body,
                             properties=pika.BasicProperties(delivery_mode=2))
            recs.append(("ok", round(time.time() - t0, 3), ""))
        except Exception as e:
            recs.append(("FAIL", round(time.time() - t0, 3),
                         f"{type(e).__name__}: {str(e)[:200]}"))
    return recs, attempted


def publish_batch(port, routing_key, n, label, conn_name="c05-owner-pub"):
    """新建连接与新的信道发布。异常不抛，全部留档。"""
    try:
        c = connect(port, conn_name)
    except Exception as e:
        return [("CONN_FAIL", 0.0, f"{type(e).__name__}: {str(e)[:120]}")] * n
    ch = c.channel()
    ch.confirm_delivery()
    recs, attempted = publish_on_channel(ch, routing_key, n, label)
    if attempted < n:
        recs += [("NOT_ATTEMPTED", 0.0, "信道已被关闭，未再尝试")] * (n - attempted)
    try:
        c.close()
    except Exception:
        pass
    return recs


def report_batch(label, recs):
    ok = [r for r in recs if r[0] == "ok"]
    bad = [r for r in recs if r[0] not in ("ok",)]
    log(f"  ┌─ {label}：尝试 {len(recs)} 条，拿到 confirm {len(ok)} 条，异常 {len(bad)} 条")
    if ok:
        durs = sorted(r[1] for r in ok)
        log(f"  │ confirm 耗时：中位 {durs[len(durs) // 2] * 1000:.0f}ms，"
            f"最慢 {durs[-1] * 1000:.0f}ms")
    if bad:
        kinds = {}
        for r in bad:
            kinds[r[2]] = kinds.get(r[2], 0) + 1
        for k, v in sorted(kinds.items(), key=lambda x: -x[1]):
            log(f"  │ ✗ × {v}  {k}")
        # 失败也是要等出来的：从「发出」到「拿到 NACK」的耗时，决定客户端被挂多久。
        # 只报成功耗时会把「被挂了几十秒才拒绝」这类事实整个丢掉。
        bd = sorted(r[1] for r in bad if r[0] != "NOT_ATTEMPTED")
        if bd:
            log(f"  │ 失败耗时：中位 {bd[len(bd) // 2]:.2f}s，最快 {bd[0]:.2f}s，"
                f"最慢 {bd[-1]:.2f}s（从发出到拿到 NACK）")
    log("  └─")
    return {"ok": len(ok), "bad": len(bad)}


def drain(qname, port, limit=500):
    """消费干净并计数 —— 证明消息真的可投递，而不只是计数器的数字。"""
    got = []
    try:
        c = connect(port, f"c05-owner-drain-{qname[-8:]}")
        ch = c.channel()
        while len(got) < limit:
            m, props, body = ch.basic_get(qname, auto_ack=True)
            if m is None:
                break
            got.append(json.loads(body.decode()).get("label"))
        c.close()
    except Exception as e:
        log(f"    消费 {qname} 时异常：{type(e).__name__}: {str(e)[:120]}")
    return got


# ------------------------------------------------------------------ 主流程

def main():
    log("=" * 78)
    log("c05 实测 [7]：classic queue 属主节点故障 ——「没有副本」vs「多数派选主」")
    log(f"环境：RabbitMQ 4.3.5 / 三节点集群 {ALL}")
    log("=" * 78)

    declare_topology()
    time.sleep(3)

    # ============================ 阶段 0：基线 ============================
    mark("【阶段 0】基线：两类队列各投 50 条持久化消息，记录故障前状态")

    # 把 quorum 的 Leader 摆到 VICTIM，与 classic 的属主同节点
    l0 = next((n for n, v in quorum_status().items() if v["state"] == "leader"), None)
    log(f"  摆放前 quorum Leader = {l0}")
    log(f"  transfer_leadership → {move_quorum_leader_to(NODE_NAME[VICTIM])}")
    time.sleep(3)
    leader, st = next(((n, v) for n, v in quorum_status().items()
                       if v["state"] == "leader"), (None, None))
    log(f"  摆放后 quorum Leader = {leader}   term = {st['term']}   "
        f"log_index = {st['log_index']}")
    assert leader == NODE_NAME[VICTIM], \
        f"quorum Leader 应被摆到 {NODE_NAME[VICTIM]}，实际 {leader}"
    log("  → 此刻两类队列的「关键节点」都是同一个节点：")
    log(f"     classic 的唯一副本在 {NODE_NAME[VICTIM]}；"
        f"quorum 的 Leader 也在 {NODE_NAME[VICTIM]}")

    log(f"\n  [0a] 基线发布：从 {NODE_NAME[VICTIM]} 各投 {N_BASE} 条")
    rp = publish_batch(NAME2PORT[VICTIM], RK_CLASSIC, N_BASE, "classic")
    rq = publish_batch(NAME2PORT[VICTIM], RK_QUORUM, N_BASE, "quorum")
    report_batch("基线·classic", rp)
    report_batch("基线·quorum", rq)

    log(f"\n  [0b] 预置一条长连接（供阶段 2 的 A 路径使用）")
    # heartbeat=0：这条连接要跨越「阶段 0 → 阶段 1 → 阶段 2」的空闲窗口。
    # 心跳开着而进程不去理它，broker 会在约 2×heartbeat 之后把连接判为无响应而关闭，
    # 那会让 A 路径退化成「在一条已死的连接上发布」，测不到「信道持有旧引用」这件事。
    c_long = connect(NAME2PORT[SURVIVOR], "c05-owner-long-lived", heartbeat=0)
    ch_long = c_long.channel()
    ch_long.confirm_delivery()
    body = json.dumps({"label": "prefault", "seq": 0,
                       "pad": "x" * (BODY - 64)}).encode()
    ch_long.basic_publish(EXCHANGE, RK_CLASSIC, body,
                          properties=pika.BasicProperties(delivery_mode=2))
    landed, waited = wait_client_node("c05-owner-long-lived", SURVIVOR)
    log(f"    连接落在 {landed}（管理 API 核实，等待 {waited}s 才采集到这条连接）")
    log(f"    已在该信道上向 classic queue 成功发布 {N_PREFAULT} 条并拿到 confirm")
    log("    用途：阶段 2 的 A 路径 = 故障前已建立、且已引用过这条队列的同一个信道")
    assert landed == NODE_NAME[SURVIVOR], f"长连接落点应为 {NODE_NAME[SURVIVOR]}，实际 {landed}"

    time.sleep(5)

    log("\n  [0c] 两类队列各自的副本范围（list_queues 的 JSON 视图）")
    for qn in (CLASSIC, QUORUM):
        r = queue_row(qn)
        log(f"    {qn}")
        log(f"      type    = {r.get('type')}")
        log(f"      leader  = {r.get('leader')}")
        log(f"      members = {ml(r.get('members'))}")
        log(f"      online  = {ml(r.get('online'))}")
        log(f"      messages= {r.get('messages')}")

    log("\n  [0d] 磁盘取证：故障前的副本落点")
    log("    (i) classic queue 的消息目录（应只在属主节点出现）")
    for c in ALL:
        d = classic_store_dir(c)
        log(f"      [{NODE_NAME[c]}] {d or '（无）'}")
    log("    (ii) classic queue 落盘目录里的文件（durability 的物理形态）")
    log(classic_dir_files(VICTIM) or "      （读不到）")
    log("    (iii) quorum queue 的 Raft 日志目录（应在三个节点各一份）")
    for c in ALL:
        n = in_container(c, "find /var/lib/rabbitmq/mnesia/rabbit@*/quorum "
                            "-mindepth 3 -maxdepth 3 -name config -type f 2>/dev/null "
                            "| wc -l").strip()
        sz = in_container(c, "du -sk /var/lib/rabbitmq/mnesia/rabbit@*/quorum/ "
                             "2>/dev/null | tail -1 | cut -f1").strip()
        log(f"      [{NODE_NAME[c]}] 队列 Raft 目录 {n} 个，quorum 总占用 {sz} KB")

    mc, _ = stable_messages(CLASSIC)
    mq, _ = stable_messages(QUORUM)
    log(f"\n  → 故障前 classic messages = {mc}，quorum messages = {mq}")
    assert mc == N_BASE + N_PREFAULT, \
        f"classic 基线应为 {N_BASE + N_PREFAULT} 条，实际 {mc}"
    assert mq == N_BASE, f"quorum 基线应为 {N_BASE} 条，实际 {mq}"
    assert len(ml(queue_row(CLASSIC).get("members"))) == 1
    assert len(ml(queue_row(QUORUM).get("members"))) == 3
    log(f"  ✓ 断言：故障前 classic {mc} 条、quorum {mq} 条；"
        "classic members=1，quorum members=3")
    log("  ✓ 断言的物理含义：这一刻同一次故障将命中 classic 的【唯一副本】"
        "与 quorum 的【三副本之一（且是 Leader）】")

    # ============================ 阶段 1：故障注入 ============================
    mark(f"【阶段 1】SIGKILL {NODE_NAME[VICTIM]}（同时是 classic 属主 + quorum Leader）")
    rc, out, err = sh(DOCKER, "kill", VICTIM, timeout=60)
    log(f"  docker kill {VICTIM}   （rc={rc}）")
    time.sleep(2)
    survivors = alive()
    log(f"  存活容器 = {survivors}")
    assert VICTIM not in survivors, f"{VICTIM} 仍在响应 ping，故障没有真正生效"

    t_kill = time.time()
    new_leader, st2, waited = None, {}, None
    while time.time() - t_kill < 60:
        st2 = quorum_status()
        cand = next((n for n, v in st2.items() if v["state"] == "leader"), None)
        if cand and cand != NODE_NAME[VICTIM]:
            new_leader, waited = cand, time.time() - t_kill
            break
        time.sleep(0.5)
    if new_leader:
        log(f"  quorum 新 Leader = {new_leader}   term = {st2[new_leader]['term']}   "
            f"（从 SIGKILL 到观察到新 Leader {waited:.2f}s；轮询粒度约 0.5s，只作本机样本）")
    else:
        log("  ⚠ 60s 内未观察到新 Leader")
    assert new_leader and new_leader != NODE_NAME[VICTIM], "quorum 未在 2/3 存活下选出新 Leader"
    log("  → 同一个瞬间：classic 失去了它唯一的副本，quorum 只是换了个 Leader")

    # ============================ 阶段 2：故障中观测 ============================
    mark("【阶段 2】故障中：客户端改连存活节点，实测两类队列各自的表现")

    probe = connect(NAME2PORT[SURVIVOR], "c05-owner-probe")
    landed, waited = wait_client_node("c05-owner-probe", SURVIVOR)
    log(f"  客户端连 {NODE_NAME[SURVIVOR]} → 管理 API 核实落点 = {landed}"
        f"（连接先保持打开，等待 {waited}s 采集到）")
    assert landed == NODE_NAME[SURVIVOR], f"客户端落点应为 {NODE_NAME[SURVIVOR]}，实际 {landed}"
    probe.close()

    log("\n  [2a] 故障中的队列视图（list_queues，从存活节点读）")
    for qn in (CLASSIC, QUORUM):
        r = queue_row(qn)
        if not r:
            log(f"    {qn}：list_queues 中看不到这一行")
            continue
        log(f"    {qn}")
        log(f"      leader  = {r.get('leader')!r}")
        log(f"      members = {ml(r.get('members'))}")
        log(f"      online  = {ml(r.get('online'))!r}")
        log(f"      messages= {r.get('messages')!r}")

    log("\n  [2b] 故障中的管理 API 队列对象（state / node / leader 字段）")
    for qn in (CLASSIC, QUORUM):
        o = api_queue(qn)
        if "_error" in o:
            log(f"    {qn}：{o['_error']}")
            continue
        log(f"    {qn}")
        for k in ("type", "state", "node", "leader", "members", "online",
                  "messages", "durable", "policy"):
            if k in o:
                log(f"      {k:<10}= {o[k]!r}")

    log("\n  [2c] 故障中在存活节点上找 classic queue 的消息目录")
    found = 0
    for ctr in ALL:
        if NODE_NAME[ctr] == NODE_NAME[VICTIM]:
            continue
        d = classic_store_dir(ctr)
        found += 1 if d else 0
        log(f"    [{NODE_NAME[ctr]}] {d or '（无 classic 消息目录）'}")
    log(f"    → 存活节点上含该 classic queue 消息目录的节点数 = {found} / 2")
    assert found == 0, "存活节点上出现了 classic queue 的副本目录，与 members=1 矛盾"
    log("    ✓ 断言：唯一副本正随故障节点离线，另外两个节点上没有任何一份")

    log(f"\n  [2d] 故障中往 classic queue 发布 —— A/B 两条路径各 {N_OUTAGE} 条")
    log("    (A) 故障前已建立、且已引用过这条队列的同一个信道"
        f"（此刻 ch_long.is_closed = {ch_long.is_closed}）")
    rec_a, att_a = publish_on_channel(ch_long, RK_CLASSIC, N_OUTAGE, "outage-A")
    if att_a < N_OUTAGE:
        rec_a += [("NOT_ATTEMPTED", 0.0, "信道已被关闭，未再尝试")] * (N_OUTAGE - att_a)
    rep_a = report_batch("故障中·classic（A 路径·同一信道）", rec_a)
    log("    (B) 故障后新建的连接与信道")
    rec_b = publish_batch(NAME2PORT[SURVIVOR], RK_CLASSIC, N_OUTAGE, "outage-B",
                          conn_name="c05-owner-pub-fresh")
    rep_b = report_batch("故障中·classic（B 路径·新信道）", rec_b)

    log(f"\n  [2e] 故障中往 quorum queue 发布 {N_OUTAGE} 条（同一个故障时刻）")
    rq_out = publish_batch(NAME2PORT[SURVIVOR], RK_QUORUM, N_OUTAGE, "quorum-outage",
                           conn_name="c05-owner-pub-quorum")
    rep_q = report_batch("故障中·quorum", rq_out)

    ok_c = rep_a["ok"] + rep_b["ok"]
    if ok_c > 0:
        log(f"  ⚠ 注意：classic 在故障中仍有 {ok_c} 条拿到 confirm。"
            "这批消息是否真的进入队列，由阶段 3 的条数核对决定 —— 不以此处为准。")
    else:
        log("  → classic 在故障中的两条路径都没有拿到 confirm。")

    log("\n  [2f] 故障中从存活节点尝试消费")
    cl = drain(CLASSIC, NAME2PORT[SURVIVOR], limit=5)
    log(f"    classic：basic_get 取到 {len(cl)} 条")
    ql = drain(QUORUM, NAME2PORT[SURVIVOR], limit=5)
    log(f"    quorum ：basic_get 取到 {len(ql)} 条，样本标签 {ql[:5]}")

    # ============================ 阶段 3：恢复 ============================
    mark(f"【阶段 3】恢复 {NODE_NAME[VICTIM]}，核对两类队列各恢复成什么样")
    sh(DOCKER, "start", VICTIM, timeout=90)
    al = wait_alive(3, timeout=180)
    log(f"  存活容器 = {al}")
    on = wait_online(3, timeout=210)
    log(f"  quorum online 副本数 = {on} / 3")

    # 追平不能只看「容器起来了」：要等三份 Raft 日志索引一致，并报出等待时长。
    # 这正是「容器恢复 ≠ 数据追平」这条要求要防的坑。
    t_wait = time.time()
    st3 = quorum_status()
    while time.time() - t_wait < 60:
        if len(st3) == 3 and len({v["log_index"] for v in st3.values()}) == 1:
            break
        time.sleep(3)
        st3 = quorum_status()
    dt_wait = time.time() - t_wait
    log(f"  quorum 各副本 Last Log Index（online 达到 3/3 后又观察了 {dt_wait:.1f}s）：")
    for n, v in sorted(st3.items()):
        log(f"    {n:<22} state={v['state']:<9} term={v['term']}  log_index={v['log_index']}")
    idx = {v["log_index"] for v in st3.values()}
    log(f"  → 三份 log_index {'一致' if len(idx) == 1 else '不一致'}：{sorted(idx)}")

    log("\n  [3a] 恢复后两类队列的条数 —— 这是「confirm 是否等于入队」的判据")
    log("      （条数指标是周期性发布的，这里等两次读数一致再取值）")
    mc2, nr_c = stable_messages(CLASSIC)
    mq2, nr_q = stable_messages(QUORUM)
    log(f"      classic 稳定读数 {mc2}（连读 {nr_c} 次一致），"
        f"quorum 稳定读数 {mq2}（连读 {nr_q} 次一致）")
    exp_c = mc + ok_c - len(cl)
    log(f"    classic messages = {mc2}")
    log(f"      故障前 {mc} 条 + 故障中两条路径拿到 confirm 共 {ok_c} 条 "
        f"- 故障中已消费 {len(cl)} 条 = 期望 {exp_c} 条")
    if mc2 == exp_c:
        log("      ✓ 条数吻合 —— 故障中拿到 confirm 的消息确实进了队列，confirm 没有落空")
    else:
        log(f"      ⚠ 缺口 {exp_c - mc2} 条 —— 存在「confirm 未兑现为入队」的情况，"
            "结论必须按这个缺口写，不能沿用「confirm 即入队」的说法")
    log(f"    quorum  messages = {mq2}")
    log(f"      故障前 {mq} 条 + 故障中 confirm {rep_q['ok']} 条 "
        f"- 故障中已消费 {len(ql)} 条 = 期望 {mq + rep_q['ok'] - len(ql)} 条")
    if mq2 == mq + rep_q["ok"] - len(ql):
        log("      ✓ 条数吻合")
    else:
        log(f"      ⚠ 与期望不符（期望 {mq + rep_q['ok'] - len(ql)}）")

    log("\n  [3b] 消费核对：消息是否真的可投递（不只是计数器的数字）")
    got_c = drain(CLASSIC, NAME2PORT[VICTIM])
    log(f"    classic 实际消费到 {len(got_c)} 条，"
        f"标签分布 { {l: got_c.count(l) for l in sorted(set(got_c))} }")
    got_q = drain(QUORUM, NAME2PORT[VICTIM])
    log(f"    quorum  实际消费到 {len(got_q)} 条（本阶段），"
        f"加上故障中已消费 {len(ql)} 条，共 {len(got_q) + len(ql)} 条")

    log("\n  [3c] 恢复后磁盘状态")
    for c in ALL:
        log(f"    [{NODE_NAME[c]}] classic 消息目录：{classic_store_dir(c) or '（无）'}")
    log("    classic 落盘目录里的文件（与故障前对照，看 SIGKILL 前后是否同一批）：")
    log(classic_dir_files(VICTIM) or "      （读不到）")
    for c in ALL:
        sz = in_container(c, "du -sk /var/lib/rabbitmq/mnesia/rabbit@*/quorum/ "
                             "2>/dev/null | tail -1 | cut -f1").strip()
        log(f"    [{NODE_NAME[c]}] quorum 总占用 {sz} KB")

    # ============================ 小结 ============================
    log("\n" + "=" * 78)
    log("小结：同一次故障（节点 1 被 SIGKILL），两类队列的可观察结果")
    log("=" * 78)
    log(f"  故障前：classic 唯一副本在 {NODE_NAME[VICTIM]}；"
        f"quorum 三副本，Leader 也在 {NODE_NAME[VICTIM]}")
    log(f"  classic：故障中 A 路径 confirm {rep_a['ok']}/{N_OUTAGE}、"
        f"B 路径 confirm {rep_b['ok']}/{N_OUTAGE}；故障中消费取到 {len(cl)} 条；"
        f"恢复后条数 {mc2}（故障前 {mc}）")
    log(f"  quorum ：故障中 confirm {rep_q['ok']}/{N_OUTAGE}、异常 {rep_q['bad']}；"
        f"故障中消费取到 {len(ql)} 条；恢复后条数 {mq2}，online {on}/3")
    log("  → 同一个动作、同一个故障时刻，差别只来自"
        "「这条队列在几个节点上有副本」")

    try:
        c_long.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
