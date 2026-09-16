#!/usr/bin/env python3
"""
c05 实测组 [3]：客户端接入节点 ≠ 队列 Leader。

对应文章：C05 第 3 节
（「客户端连的是哪台」与「队列 Leader 在哪台」是两个独立的事）。

要回答的问题：
  1. 把 quorum queue 的 Leader 明确摆在 node2，客户端连 node1，pub 还能成功吗？
  2. 客户端连的节点和 Leader 位置排列组合，有没有哪一组会失败？
  3. 「接入节点把操作转给 Leader」这件事，有没有可观测的证据，而不是靠文档推断？

设计要点：
  - 用 `rabbitmq-queues transfer_leadership` 把 Leader 【确定性地】摆到指定节点，
    不依赖默认分布，也不用猜。
  - 证据落在 Raft 日志索引上：发布前 / 发布后各读一次 quorum_status 的
    Last Log Index，增量应该等于发布条数。如果客户端是在接入节点本地写，
    这个索引不会按时增长。
  - 额外做一条 2 副本队列，客户端连【连副本都不是】的第三个节点，看是否仍成功。

运行（需先起 3 节点集群）：
    cd infra/rabbitmq-cluster
    docker compose up -d
    python3 scripts/non-leader-publish.py

实测结果（RabbitMQ 4.3.5，三节点）：见
    experiments/rabbitmq-cluster/raw-non-leader-publish.log
"""

import base64
import json
import os
import re
import shutil
import subprocess
import time
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
NAME2CTR = {v: k for k, v in NODE_NAME.items()}

EXCHANGE = "gyd.c05.nonleader.ex"
QUEUE = "gyd.c05.nonleader.quorum"
QUEUE_2REPL = "gyd.c05.nonleader.2repl"
# 两个队列必须用不同路由键：绑到同一个键上时，一条消息会同时进入两个队列，
# 各自的 messages 计数都会涨，反而看不清「一条消息在几处落盘」。
RK = "nonleader"          # 三副本队列
RK_2REPL = "nonleader.2r"  # 两副本队列

N_MSG = 100
BODY = 512

CRED = pika.PlainCredentials("admin", "admin123")
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def log(msg=""):
    print(msg, flush=True)


def sh(*args, timeout=120):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return 124, "", f"命令超时（>{timeout}s）"


def ctl(container, *args, timeout=40):
    _, out, err = sh(DOCKER, "exec", container, "rabbitmqctl", *args, timeout=timeout)
    return (out or err).strip()


def qcmd(container, *args, timeout=60):
    _, out, err = sh(DOCKER, "exec", container, "rabbitmq-queues", *args, timeout=timeout)
    return (out or err).strip()


# ------------------------------------------------------------------ 观测

def quorum_status(queue, container="gyd-rabbitmq1"):
    """返回 {node: {state, membership, log_index, commit_index, term}}"""
    raw = qcmd(container, "quorum_status", queue, "--formatter", "json")
    raw = ANSI.sub("", raw)
    start = raw.find("[")
    end = raw.rfind("]")
    if start < 0 or end < 0:
        return {}
    rows = json.loads(raw[start:end + 1])
    out = {}
    for r in rows:
        out[r["Node Name"]] = {
            "state": r["Raft State"],
            "membership": r["Membership"],
            "log_index": r["Last Log Index"],
            "commit_index": r["Commit Index"],
            "term": r["Term"],
        }
    return out


def member_list(v):
    """JSON formatter 下 members/online 已经是数组；文本 formatter 下是 '[a, b]'。"""
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    return [x.strip() for x in str(v).strip("[]").split(",") if x.strip()]


def queue_row(queue, container="gyd-rabbitmq1"):
    out = ctl(container, "-q", "list_queues", "name", "type", "leader", "members",
              "online", "messages", "--formatter", "json")
    start, end = out.find("["), out.rfind("]")
    for r in json.loads(out[start:end + 1]) if start >= 0 else []:
        if r.get("name") == queue:
            return r
    return {}


def leader_of(queue, container="gyd-rabbitmq1"):
    return queue_row(queue, container).get("leader", "?")


def api(path, port=15673):
    """管理 API：连接和队列都带一个明确的 node 字段，比猜地址可靠。"""
    req = urllib.request.Request(f"http://127.0.0.1:{port}/api/{path}")
    req.add_header("Authorization", "Basic " + base64.b64encode(
        f"{CRED.username}:{CRED.password}".encode()).decode())
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return json.loads(opener.open(req, timeout=15).read())


def connections():
    """返回 [(connection_name, node, state)]，只取本实验自建的连接。"""
    out = []
    for c in api("connections"):
        nm = next((v for k, v in (c.get("client_properties") or {}).items()
                   if k == "connection_name"), None)
        if nm and str(nm).startswith("c05"):
            out.append((nm, c.get("node"), c.get("state")))
    return out


# ------------------------------------------------------------------ 拓扑

def connect(port, name=""):
    props = {"connection_name": name} if name else None
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost", port=port, credentials=CRED,
        client_properties=props, heartbeat=30, blocked_connection_timeout=15))


def drop(port):
    c = connect(port)
    ch = c.channel()
    for q in (QUEUE, QUEUE_2REPL):
        try:
            ch.queue_delete(q)
        except Exception:
            pass
    try:
        ch.exchange_delete(EXCHANGE)
    except Exception:
        pass
    c.close()


def declare(port, group_size):
    c = connect(port, f"c05-declarer-{group_size}")
    ch = c.channel()
    ch.exchange_declare(EXCHANGE, "direct", durable=True)
    q = QUEUE if group_size == 3 else QUEUE_2REPL
    rk = RK if group_size == 3 else RK_2REPL
    ch.queue_declare(q, durable=True, arguments={
        "x-queue-type": "quorum", "x-quorum-initial-group-size": group_size})
    ch.queue_bind(q, EXCHANGE, routing_key=rk)
    c.close()


def publish_from(port, n, label, rk=RK):
    """从指定端口发布 n 条持久化消息，返回 (成功条数, 耗时, 错误)。"""
    try:
        c = connect(port, f"c05-pub-{label}")
    except Exception as e:
        return 0, 0.0, f"连接失败: {type(e).__name__}: {e}"
    try:
        ch = c.channel()
        ch.confirm_delivery()
        body = json.dumps({"label": label, "pad": "y" * (BODY - 40)}).encode()
        t0 = time.time()
        ok = 0
        for _ in range(n):
            try:
                ch.basic_publish(EXCHANGE, rk, body,
                                 properties=pika.BasicProperties(delivery_mode=2))
                ok += 1
            except Exception as e:
                try:
                    c.close()
                except Exception:
                    pass
                return ok, time.time() - t0, f"第 {ok + 1} 条被拒: {type(e).__name__}: {e}"
        dt = time.time() - t0
        c.close()
        return ok, dt, ""
    except Exception as e:
        try:
            c.close()
        except Exception:
            pass
        return 0, 0.0, f"通道失败: {type(e).__name__}: {e}"


def drain(port, queue, max_n=5000):
    got = []
    try:
        c = connect(port, "c05-drainer")
    except Exception as e:
        return got, f"连接失败: {e}"
    ch = c.channel()
    while len(got) < max_n:
        method, _p, body = ch.basic_get(queue, auto_ack=True)
        if method is None:
            break
        try:
            got.append(json.loads(body)["label"])
        except Exception:
            got.append("?")
    c.close()
    return got, ""


# ------------------------------------------------------------------ 主流程

def main():
    log("=" * 78)
    log("c05 实测 [3]：客户端接入节点 ≠ 队列 Leader")
    log("=" * 78)

    base = NAME2PORT["gyd-rabbitmq1"]
    drop(base)
    declare(base, 3)
    declare(base, 2)
    time.sleep(3)

    # -------------------------------------------------- PART A
    log("\n【A】三副本 quorum queue：Leader 轮转 × 客户端接入节点轮转")
    rows = queue_row(QUEUE)
    log(f"  队列 {QUEUE}")
    log(f"    members = {rows.get('members')}   online = {rows.get('online')}")

    results = []
    for leader_ctr in ALL:
        target = NODE_NAME[leader_ctr]
        log(f"\n  ── 把 Leader 摆到 {target} ──")
        out = qcmd("gyd-rabbitmq1", "transfer_leadership", QUEUE, target)
        log(f"    rabbitmq-queues transfer_leadership {QUEUE} {target}")
        log(f"    → {out.splitlines()[0] if out else '(无输出)'}")
        time.sleep(2)

        st = quorum_status(QUEUE)
        actual_leader = next((n for n, v in st.items() if v["state"] == "leader"), "?")
        log(f"    实际 Leader = {actual_leader}")
        assert actual_leader == target, \
            f"Leader 未按要求摆放：期望 {target}，实际 {actual_leader}"
        log(f"    各副本 Raft 状态：" + "  ".join(
            f"{n}={v['state']}(log={v['log_index']},term={v['term']})"
            for n, v in sorted(st.items())))

        for entry_ctr in ALL:
            entry = NODE_NAME[entry_ctr]
            before = quorum_status(QUEUE)
            before_log = before[actual_leader]["log_index"]

            ok, dt, err = publish_from(NAME2PORT[entry_ctr], N_MSG, f"A-{entry}")

            time.sleep(2)
            after = quorum_status(QUEUE)
            after_log = after[actual_leader]["log_index"]
            delta = after_log - before_log

            same = "同一台" if entry == actual_leader else "不同台"
            log(f"      客户端连 {entry:<20} {same:<6} "
                f"发布 {ok}/{N_MSG} 条 ok  {dt:6.3f}s  "
                f"Leader log {before_log}→{after_log} (Δ{delta})  {err}")
            results.append({"leader": actual_leader, "entry": entry, "ok": ok,
                            "dt": dt, "delta": delta, "err": err})

    log("\n  [A 汇总]")
    log(f"    {'Leader':<20}{'接入节点':<20}{'结果':<10}{'耗时':<10}{'log Δ':<8}")
    for r in results:
        log(f"    {r['leader']:<20}{r['entry']:<20}"
            f"{r['ok']}/{N_MSG:<8}{r['dt']:<10.3f}{r['delta']:<8}")

    assert all(r["ok"] == N_MSG and not r["err"] for r in results), \
        f"存在发布失败：{[r for r in results if r['ok'] != N_MSG or r['err']]}"
    log("  ✓ 断言：9 组（3 个 Leader 位置 × 3 个接入节点）全部 100% 拿到 confirm")

    assert all(r["delta"] == N_MSG + 2 for r in results), \
        f"Raft 日志增量与预期不符：{[(r['leader'], r['entry'], r['delta']) for r in results]}"
    log(f"  ✓ 断言：每一组的 Leader Raft 日志增量都是 {N_MSG}+2 —— "
        f"提交确实发生在 Leader 上，与客户端连哪台无关")
    log(f"     那 +2 不是消息，而是发布方信道在队列状态机里的登记与注销：")
    log(f"       每个信道首次发布 → register_enqueuer（+1）；关闭信道 → unregister_enqueuer（+1）")
    log(f"       登记是按【信道】计，不是按连接：同一条连接上新开一个信道会再登记一次，")
    log(f"       关掉整条连接而信道已先关闭时不再追加。")
    log(f"       → quorum queue 的 Raft 日志里不只有消息，还有队列操作本身")

    # 取回验证：从一个【非 Leader】节点取
    cur_leader = leader_of(QUEUE)
    non_leader_ctr = next(c for c in ALL if NODE_NAME[c] != cur_leader)
    got, err = drain(NAME2PORT[non_leader_ctr], QUEUE)
    log(f"\n  从非 Leader 节点 {NODE_NAME[non_leader_ctr]} 取回 {len(got)} 条  {err}")
    assert len(got) == len(results) * N_MSG, \
        f"应取回 {len(results) * N_MSG} 条，实际 {len(got)}"

    # -------------------------------------------------- PART B
    log("\n\n【B】两副本 quorum queue：客户端连【连副本都不是】的第三个节点")
    rows2 = queue_row(QUEUE_2REPL)
    members2 = member_list(rows2.get("members", []))
    log(f"  队列 {QUEUE_2REPL}")
    log(f"    members = {members2}   （{len(members2)} 个副本）")
    outsider = next((n for n in NODE_NAME.values() if n not in members2), None)
    log(f"    非成员节点 = {outsider}")
    assert outsider is not None, f"两副本队列应该留下一个非成员节点，实际 members={members2}"

    entry_ctr = NAME2CTR[outsider]
    before = quorum_status(QUEUE_2REPL)
    b_leader = next((n for n, v in before.items() if v["state"] == "leader"), "?")
    b_log = before[b_leader]["log_index"]

    ok, dt, err = publish_from(NAME2PORT[entry_ctr], N_MSG, f"B-{outsider}", rk=RK_2REPL)
    time.sleep(2)
    after = quorum_status(QUEUE_2REPL)
    log(f"    客户端连 {outsider}（不是副本成员）发布 {N_MSG} 条 → 成功 {ok} 条，{dt:.3f}s  {err}")
    log(f"    Leader {b_leader} 的 log {b_log}→{after[b_leader]['log_index']} "
        f"(Δ{after[b_leader]['log_index'] - b_log})")
    log(f"    各副本状态：" + "  ".join(
        f"{n}={v['state']}(log={v['log_index']})" for n, v in sorted(after.items())))

    assert ok == N_MSG and not err, f"非成员节点发布失败：ok={ok}, err={err}"
    log("  ✓ 断言：接入节点连副本都不是，发布照样成功 —— 接入节点只是入口")

    got2, _ = drain(NAME2PORT[NAME2CTR[members2[0]]], QUEUE_2REPL)
    log(f"    从副本节点 {members2[0]} 取回 {len(got2)} 条")
    assert len(got2) == N_MSG, f"应取回 {N_MSG} 条，实际 {len(got2)}"

    # -------------------------------------------------- PART C
    log("\n\n【C】那 +2 到底是什么：把登记粒度钉到信道级")
    log("  做法：在同一条连接上开 / 关信道，逐次读 Leader 的 Last Log Index。")
    c = connect(NAME2PORT["gyd-rabbitmq1"], "c05-c-probe")
    ch_a = c.channel()
    ch_a.confirm_delivery()

    def _log():
        st = quorum_status(QUEUE)
        return next((v["log_index"] for v in st.values() if v["state"] == "leader"), -1)

    def _pub(ch, n):
        body = json.dumps({"pad": "c" * 100}).encode()
        for _ in range(n):
            ch.basic_publish(EXCHANGE, RK, body,
                             properties=pika.BasicProperties(delivery_mode=2))

    l0 = _log()
    _pub(ch_a, 1)
    time.sleep(1.2)
    l1 = _log()
    log(f"    信道 A 第 1 条            : log {l0}→{l1}  Δ={l1 - l0}")
    _pub(ch_a, 1)
    time.sleep(1.2)
    l2 = _log()
    log(f"    信道 A 第 2 条            : log {l1}→{l2}  Δ={l2 - l1}")
    ch_a.close()
    time.sleep(1.5)
    l3 = _log()
    log(f"    只关信道 A（连接仍在）    : log {l2}→{l3}  Δ={l3 - l2}")
    ch_b = c.channel()
    ch_b.confirm_delivery()
    _pub(ch_b, 1)
    time.sleep(1.2)
    l4 = _log()
    log(f"    同连接上新开信道 B 第 1 条: log {l3}→{l4}  Δ={l4 - l3}")
    ch_b.close()
    time.sleep(1.5)
    l5 = _log()
    log(f"    只关信道 B                : log {l4}→{l5}  Δ={l5 - l4}")
    c.close()
    time.sleep(1.5)
    l6 = _log()
    log(f"    关掉整条连接              : log {l5}→{l6}  Δ={l6 - l5}")

    assert (l1 - l0, l2 - l1, l3 - l2, l4 - l3, l5 - l4, l6 - l5) == (2, 1, 1, 2, 1, 0), \
        f"信道级登记的证据形状不符：{(l1-l0, l2-l1, l3-l2, l4-l3, l5-l4, l6-l5)}"
    log("  ✓ 断言：形状 = (首次发布 2, 同信道再发 1, 关信道 1, 新信道首次 2, 关信道 1, 关连接 0)")
    log("     → 每个信道首次发布时在队列状态机里登记一次，关闭信道时注销一次；")
    log("       按信道计，不按连接计。这 2 条不是消息，是队列操作。")

    # -------------------------------------------------- PART D
    log("\n\n【D】客户端接入点 / 队列 Leader 在 RabbitMQ 侧的落点")
    log("  做法：保持两条连接打开，再用管理 API 读它们的 node 字段。")
    keep = [connect(NAME2PORT["gyd-rabbitmq1"], "c05-keep-on-5673"),
            connect(NAME2PORT["gyd-rabbitmq3"], "c05-keep-on-5675")]
    for k in keep:
        k.channel()
    time.sleep(2)

    conns = connections()
    log("    连接落点（管理 API /api/connections 的 node 字段）：")
    for nm, node, state in conns:
        log(f"      {nm:<24} → node={node}  state={state}")
    got_nodes = {node for _nm, node, _s in conns}
    assert got_nodes == {NODE_NAME["gyd-rabbitmq1"], NODE_NAME["gyd-rabbitmq3"]}, \
        f"两条连接应分别落在 node1 / node3，实际 {got_nodes}"
    log("    ✓ 断言：连接确实落在客户端所连的那一台")

    log("\n    队列视图（管理 API /api/queues）：")
    for q in api("queues"):
        if not str(q.get("name", "")).startswith("gyd.c05."):
            continue
        log(f"      {q['name']:<30} type={q.get('type'):<8} "
            f"node={q.get('node')}  leader={q.get('leader')}  "
            f"members={q.get('members')}")
    log("    ↑ classic 队列只有 node（属主），没有 leader/members；")
    log("      quorum 队列同时有 node（该视图由哪台提供）和 leader/members。")

    for k in keep:
        k.close()

    # -------------------------------------------------- 小结
    log("\n" + "=" * 78)
    log("小结")
    log("=" * 78)
    log("  1. Leader 用 transfer_leadership 明确摆放，9 组排列组合全部成功；")
    log("  2. 每组的 Leader Raft 日志增量 = 发布条数 + 2（信道登记/注销），")
    log("     提交点在 Leader，不在接入节点；")
    log("  3. 接入节点连副本都不是时，发布仍然成功；")
    log("  4. 消息可从任一节点（含非 Leader）取回；")
    log("  5. 登记按信道计而非按连接计 —— quorum queue 的日志里也有「操作」本身。")
    log("  → 客户端接入点与队列 Leader 位置解耦，代价是每次发布都要跨一次节点边界。")
    log("\n[清理] 保留拓扑供后续实验复用。")


if __name__ == "__main__":
    main()
