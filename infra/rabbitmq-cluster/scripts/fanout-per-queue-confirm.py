#!/usr/bin/env python3
"""
c05 实测组 [8]：一条消息路由到多个队列 —— confirm 是「所有队列都接受」的合成结果。

对应文章：C05 第 9 节（Publisher Confirm 的三条边界）与第 7 节（消息的写路径）

要回答的问题（纲领「必须覆盖的机制范围」第 3 项最后一条）：
  「一条消息路由到多个队列时，每个队列分别决定是否接受，
    不能把它写成一次全局落盘。」
  这条如果不实测，就只能引用官方原词；本轮把它做成可观察对照。

设计要点（三处刻意为之）：
  · 【一个 exchange，两个队列，两种归属】fanout exchange 同时绑 classic queue
    与 quorum queue。消息只有一条，路由结果必然是两个队列都命中。
  · 【只打掉其中一个队列的可用性】把 quorum 的 Leader 先搬到节点 2，
    再 SIGKILL 节点 1 —— 于是 classic queue 失去唯一副本，
    而 quorum queue 仍有 2/3 多数派。同一个故障时刻，
    两个队列对【同一条消息】的接受能力被分开成「能」与「不能」。
  · 【把三件事分开记】一条消息的结局有三层，混在一起就白测：
      1) 客户端拿到什么（ack / nack / return / 挂住）；
      2) quorum queue 那边到底有没有收到 —— 用条数判定，不用客户端结果推断；
      3) classic queue 恢复后有没有这条消息。
    第 2 层是本实验的核心：客户端没拿到 confirm，不等于消息哪儿都没进去。

运行（需先起 3 节点集群）：
    cd infra/rabbitmq-cluster
    docker compose up -d
    python3 scripts/fanout-per-queue-confirm.py

实测结果（RabbitMQ 4.3.5，三节点）：见
    experiments/rabbitmq-cluster/raw-fanout-per-queue.log
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

EXCHANGE = "gyd.c05.fanout.ex"
CLASSIC = "gyd.c05.fanout.classic"
QUORUM = "gyd.c05.fanout.quorum"

VICTIM = "gyd-rabbitmq1"        # classic queue 的归属节点（拓扑就在它上面声明）
SURVIVOR = "gyd-rabbitmq2"      # 故障期间客户端改连这里，也是 quorum Leader 的新位置

N_BASE = 10                     # 基线：一条消息同时进两个队列
N_OUTAGE = 10                   # 故障中：同一条消息，两个队列接受能力不同
N_QUORUM_ONLY = 5               # 对照：只往 quorum queue 发，证明它不是瓶颈
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


def stable_messages(qname, timeout=60, gap=5):
    """等条数指标稳定后再取值。

    队列的 messages 是【周期性采集并发布】的指标，不是读取瞬间的真值
    （quorum queue 上由 Ra tick 回调上报，默认 5s 一次）。凡是拿条数当判据，
    都必须等两次读数一致 —— 否则会把「还没采集到」读成「没进去」。
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


def wait_quorum_online(n, timeout=210):
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


def wait_classic_home(ctr_node_name, timeout=240):
    """等 classic queue 重新有归属。

    classic queue 没有 Raft，被杀的属主节点回来之前它一直「不在」。
    判据只用 list_queues 的 leader 字段（classic 的 leader 就是属主节点），
    不用管理 API 的 state —— 实测 [7] 阶段 2b 已证明那个字段在故障期间
    仍报 'running' 并指向已离线节点。
    """
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        r = queue_row(CLASSIC)
        if r:
            last = str(r.get("leader", "")).strip()
            if last == ctr_node_name:
                return last
        time.sleep(3)
    return last


# ------------------------------------------------------------------ 拓扑

def connect(port, name="", heartbeat=10):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost", port=port, credentials=CRED,
        client_properties={"connection_name": name} if name else None,
        heartbeat=heartbeat, blocked_connection_timeout=25, connection_attempts=1))


def declare_topology():
    """在 VICTIM 节点上声明整条拓扑 —— 目的是让 classic queue 的属主就在那里。"""
    c = connect(NAME2PORT[VICTIM], "c05-fanout-declarer")
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
    ch.exchange_declare(EXCHANGE, "fanout", durable=True)
    ch.queue_declare(CLASSIC, durable=True, arguments={"x-queue-type": "classic"})
    ch.queue_declare(QUORUM, durable=True, arguments={
        "x-queue-type": "quorum", "x-quorum-initial-group-size": 3})
    ch.queue_bind(CLASSIC, EXCHANGE)      # fanout：routing key 被忽略，两个队列都命中
    ch.queue_bind(QUORUM, EXCHANGE)
    c.close()


def move_quorum_leader_to(node_name):
    """把 quorum queue 的 Leader 摆到指定节点 —— 让两个队列的可用性分岔。

    两处 4.3.5 CLI 的真实行为（实验 [7] 已确认，这里沿用）：
      · 目标节点是【位置参数】，不是 `--node`；
      · 目标已是 Leader 时返回 rc=69 并打印 already the leader，属目标状态已满足。
    """
    target = node_name
    last = ""
    for c in ALL:
        rc, out, err = sh(DOCKER, "exec", c, "rabbitmq-queues", "transfer_leadership",
                          QUORUM, target, timeout=30)
        last = ((out or "") + " " + (err or "")).strip()
        if rc == 0:
            return last.replace("\n", " ")[:110]
        if "already the leader" in last:
            return "目标节点已经是 Leader（rc=69 already the leader，无需搬迁）"
    return f"transfer_leadership 未能确认（{last[:100]}）"


# ------------------------------------------------------------------ 发布

def publish_fanout(port, n, label, conn_name):
    """向 fanout 发布 n 条，逐条记结果与耗时。

    mandatory=True 是有意的：它把「消息被退回」与「消息被接受」分成两种
    可区分的结局。若某条消息只被部分队列命中，客户端应当看到的是
    confirm 结果的变化，而不是 basic.return —— 这两者不能混为一谈。
    """
    recs, attempted = [], 0
    try:
        c = connect(port, conn_name)
    except Exception as e:
        return [("CONN_FAIL", 0.0, f"{type(e).__name__}: {str(e)[:120]}")] * n
    ch = c.channel()
    ch.confirm_delivery()
    for i in range(n):
        if getattr(ch, "is_closed", False):
            break
        body = json.dumps({"label": label, "seq": i,
                           "pad": "x" * (BODY - 64)}).encode()
        t0 = time.time()
        attempted += 1
        try:
            ch.basic_publish(EXCHANGE, "", body, mandatory=True,
                             properties=pika.BasicProperties(delivery_mode=2))
            recs.append(("ok", round(time.time() - t0, 3), ""))
        except Exception as e:
            recs.append(("FAIL", round(time.time() - t0, 3),
                         f"{type(e).__name__}: {str(e)[:200]}"))
    if attempted < n:
        recs += [("NOT_ATTEMPTED", 0.0, "信道已被关闭，未再尝试")] * (n - attempted)
    try:
        c.close()
    except Exception:
        pass
    return recs


def publish_direct(port, qname, n, label, conn_name):
    """对照：只往 quorum queue 发（同一台机器、同一时刻）。"""
    recs, attempted = [], 0
    try:
        c = connect(port, conn_name)
    except Exception as e:
        return [("CONN_FAIL", 0.0, f"{type(e).__name__}: {str(e)[:120]}")] * n
    ch = c.channel()
    ch.confirm_delivery()
    for i in range(n):
        if getattr(ch, "is_closed", False):
            break
        body = json.dumps({"label": label, "seq": i,
                           "pad": "x" * (BODY - 64)}).encode()
        t0 = time.time()
        attempted += 1
        try:
            ch.basic_publish("", qname, body,
                             properties=pika.BasicProperties(delivery_mode=2))
            recs.append(("ok", round(time.time() - t0, 3), ""))
        except Exception as e:
            recs.append(("FAIL", round(time.time() - t0, 3),
                         f"{type(e).__name__}: {str(e)[:200]}"))
    if attempted < n:
        recs += [("NOT_ATTEMPTED", 0.0, "信道已被关闭，未再尝试")] * (n - attempted)
    try:
        c.close()
    except Exception:
        pass
    return recs


def report_batch(label, recs):
    ok = [r for r in recs if r[0] == "ok"]
    bad = [r for r in recs if r[0] != "ok"]
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
        bd = sorted(r[1] for r in bad if r[0] != "NOT_ATTEMPTED")
        if bd:
            log(f"  │ 失败耗时：中位 {bd[len(bd) // 2]:.2f}s，最快 {bd[0]:.2f}s，"
                f"最慢 {bd[-1]:.2f}s（从发出到拿到结局）")
    log("  └─")
    return {"ok": len(ok), "bad": len(bad)}


def drain(qname, port, limit=200):
    """消费干净并计数 —— 证明消息真的可投递，而不只是计数器的数字。"""
    got = []
    try:
        c = connect(port, f"c05-fanout-drain-{qname[-6:]}")
        ch = c.channel()
        while len(got) < limit:
            m, props, body = ch.basic_get(qname, auto_ack=True)
            if m is None:
                break
            got.append(json.loads(body.decode()).get("label"))
        c.close()
    except Exception as e:
        got.append(f"<异常 {type(e).__name__}: {str(e)[:120]}>")
    return got


def tails(lst, k=12):
    return lst if len(lst) <= k else lst[:k] + [f"...(共 {len(lst)} 条)"]


# ------------------------------------------------------------------ 主流程

def main():
    log("=" * 78)
    log("c05 实测组 [8]：一条消息路由到两个队列时，confirm 是谁替谁下的结论")
    log(f"  拓扑          : fanout exchange {EXCHANGE}")
    log(f"                  ├─ {CLASSIC}（classic，单副本）")
    log(f"                  └─ {QUORUM}（quorum，3 副本）")
    log(f"  故障          : SIGKILL {VICTIM}（classic 属主 / quorum 非 Leader）")
    log("=" * 78)

    mark("阶段 0：确认三节点存活，建立拓扑")
    log(f"  存活节点：{alive()}")
    declare_topology()
    log("  拓扑已声明（在节点 1 上声明，classic queue 的属主因此是节点 1）")

    moved = move_quorum_leader_to(NODE_NAME[SURVIVOR])
    log(f"  quorum Leader 搬迁：{moved}")

    rc = queue_row(CLASSIC)
    rq = queue_row(QUORUM)
    log(f"  classic  leader={str(rc.get('leader','?')).strip()}  members={ml(rc.get('members'))}")
    log(f"  quorum   leader={str(rq.get('leader','?')).strip()}  members={ml(rq.get('members'))}")

    classic_home = str(rc.get("leader", "")).strip()
    quorum_leader = str(rq.get("leader", "")).strip()
    log(f"\n  断言：classic 属主 == {NODE_NAME[VICTIM]} → "
        f"{'✓' if classic_home == NODE_NAME[VICTIM] else '✗ ' + classic_home}")
    log(f"  断言：quorum Leader == {NODE_NAME[SURVIVOR]} → "
        f"{'✓' if quorum_leader == NODE_NAME[SURVIVOR] else '✗ ' + quorum_leader}")
    log("  → 一句话：接下来那一次 SIGKILL，对 classic 是「唯一副本消失」，"
        "对 quorum 只是「换个 Leader」。")

    # -------------------------------------------------------------- 基线
    mark(f"阶段 1：基线 —— 从节点 2 发 {N_BASE} 条，一条消息同时命中两个队列")
    log("  （客户端连节点 2；classic 的属主在节点 1、quorum 的 Leader 也在节点 2）")
    rep = report_batch("基线（fanout → classic + quorum）",
                       publish_fanout(NAME2PORT[SURVIVOR], N_BASE, "base",
                                      "c05-fanout-base"))
    time.sleep(1)
    c_base, n_base = stable_messages(CLASSIC)
    q_base, n_qbase = stable_messages(QUORUM)
    log(f"  classic 条数 = {c_base}（读 {n_base} 次稳定）")
    log(f"  quorum  条数 = {q_base}（读 {n_qbase} 次稳定）")
    log(f"  断言：一条消息进两个队列，所以两个条数都等于 confirm 数 "
        f"{N_BASE} → {'✓' if c_base == q_base == rep['ok'] == N_BASE else '✗ 见上表'}")

    # -------------------------------------------------------------- 故障
    mark(f"阶段 2：SIGKILL {VICTIM}")
    t_kill = time.time()
    rc_k, _, _ = sh(DOCKER, "kill", "--signal=KILL", VICTIM, timeout=30)
    log(f"  docker kill rc={rc_k}")
    time.sleep(8)
    log(f"  存活节点：{alive()}")
    rq2 = queue_row(QUORUM)
    rc2 = queue_row(CLASSIC)
    log(f"  quorum online = {ml(rq2.get('online'))}  leader = {str(rq2.get('leader','?')).strip()}")
    log(f"  classic leader = {str(rc2.get('leader','?')).strip()}"
        f"  （classic 没有选举，属主不在就是不在）")

    mark(f"阶段 3：故障中，往 fanout 发 {N_OUTAGE} 条 —— 同一条消息，两个队列命运不同")
    rep_out = report_batch("故障中（fanout → classic 离线 + quorum 2/3 在线）",
                           publish_fanout(NAME2PORT[SURVIVOR], N_OUTAGE, "outage",
                                          "c05-fanout-outage"))
    log("  注意：客户端拿到的是【一条消息一个结局】，不是两条。")
    log("        下面第 2 层读数证明这个结局是谁导致的。")

    mark("阶段 3b：核心读数 —— 客户端没拿到 confirm，quorum queue 那边到底有没有收到？")
    time.sleep(2)
    q_out, n_qout = stable_messages(QUORUM)
    log(f"  quorum  条数 = {q_out}（读 {n_qout} 次稳定），基线是 {q_base}")
    delta_q = None if (q_out is None or q_base is None) else q_out - q_base
    log(f"  quorum  增量 = {delta_q}，而客户端拿到 confirm 的条数是 {rep_out['ok']}")
    if delta_q == rep_out["ok"]:
        log("  → 两条数一致：quorum queue 收到多少，客户端就确认多少。"
            "这一轮 quorum 全程接受了消息。")
    elif delta_q is not None and delta_q > rep_out["ok"]:
        log(f"  → ★ 关键读数：quorum queue 多收了 {delta_q - rep_out['ok']} 条"
            " —— 消息进了队列，客户端却没有 confirm。")
        log("    这就是「每个队列分别决定是否接受」：confirm 是【所有命中队列】"
            "都接受之后才发的合成结果，")
        log("    不是「某个队列接受了」的计数，也不是一次全局落盘。")
    else:
        log("  → 两条数对不上且方向异常，如实留档，不解释。")

    mark(f"阶段 3c：对照 —— 同一时刻只往 quorum queue 发 {N_QUORUM_ONLY} 条")
    log("  若这一组全部成功，就证明故障中失败的原因不是「客户端连不上」"
        "也不是「quorum 不可用」。")
    rep_ctrl = report_batch("故障中（direct → 只发 quorum）",
                            publish_direct(NAME2PORT[SURVIVOR], QUORUM,
                                           N_QUORUM_ONLY, "ctrl", "c05-fanout-ctrl"))
    time.sleep(2)
    q_ctrl, n_qctrl = stable_messages(QUORUM)
    log(f"  quorum  条数 = {q_ctrl}（读 {n_qctrl} 次稳定）")

    # -------------------------------------------------------------- 恢复
    mark(f"阶段 4：重启 {VICTIM}，看 classic queue 回来时里面有什么")
    sh(DOCKER, "start", VICTIM, timeout=60)
    live = wait_alive(3, timeout=180)
    log(f"  存活节点：{live}")
    home = wait_classic_home(NODE_NAME[VICTIM], timeout=240)
    log(f"  classic 属主已回到 {home}")

    c_fin, n_cfin = stable_messages(CLASSIC)
    q_fin, n_qfin = stable_messages(QUORUM)
    log(f"  classic 条数 = {c_fin}（读 {n_cfin} 次稳定）")
    log(f"  quorum  条数 = {q_fin}（读 {n_qfin} 次稳定）")

    mark("阶段 5：把两个队列消费干净 —— 证明条数不是计数器幻觉")
    got_c = drain(CLASSIC, NAME2PORT[VICTIM], limit=200)
    got_q = drain(QUORUM, NAME2PORT[SURVIVOR], limit=200)
    log(f"  classic 消费到 {len(got_c)} 条，标签分布 "
        f"{ {l: got_c.count(l) for l in sorted(set(got_c))} }")
    log(f"  quorum  消费到 {len(got_q)} 条，标签分布 "
        f"{ {l: got_q.count(l) for l in sorted(set(got_q))} }")
    log(f"  标签：{tails(got_c)}")
    log(f"  标签：{tails(got_q)}")

    # -------------------------------------------------------------- 汇总
    mark("汇总")
    log(f"  {'阶段':<38}{'客户端 confirm':>16}{'quorum 增量':>14}{'classic 增量':>14}")
    log(f"  {'基线 fanout→两队列':<38}{rep['ok']:>16}"
        f"{(q_base - 0 if q_base is not None else -1):>14}"
        f"{(c_base - 0 if c_base is not None else -1):>14}")
    log(f"  {'故障中 fanout→两队列（classic 属主离线）':<38}{rep_out['ok']:>16}"
        f"{'-' if delta_q is None else delta_q:>14}{'n/a（属主离线）':>14}")
    log(f"  {'故障中 direct→只发 quorum（对照）':<38}{rep_ctrl['ok']:>16}"
        f"{'-' if (q_ctrl is None or q_out is None) else q_ctrl - q_out:>14}{'-':>14}")
    log("")
    log("  判据说明（必须三条一起看，缺一条就会得出错误结论）：")
    log("    1) 客户端 confirm：一条消息一个结局，等于所有命中队列的 AND；")
    log("    2) quorum 增量：故障中它是否仍在独立接受消息；")
    log("    3) 对照：同一时刻只发 quorum 是否成功 —— 排除「客户端/网络不行」。")
    log("=" * 78)


if __name__ == "__main__":
    main()
