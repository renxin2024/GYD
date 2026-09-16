#!/usr/bin/env python3
"""
c05 实测组 [4][5][6]：三类故障的完整时间线。

对应文章：C05 第 5 节（故障时间线）
要回答的问题：
  - Leader 被终止后，还在发布的客户端会看到什么？停顿多久？谁接任？
  - 「突然失联」和「优雅停止」是不是同一种故障？
  - 接入节点被终止（队列 Leader 还活着）时，客户端看到什么？
  - 三副本只剩一个、失去多数派时，客户端会怎样？
  - 节点回来之后，靠什么判断副本真的追平了？

三处容易被做错、这里刻意分开处理的地方：
  1. 【客户端到底连在哪台】必须核实。脚本用 Publisher.move_to() 强制断开重连，
     再用管理 API /api/connections 的 node 字段验证；验证不过就中止该阶段。
     否则「停掉接入节点」可能停的是一台与客户端无关的机器（v1 就栽在这里）。
  2. 【选举停顿】要测的是「某一次发布被挂住多久」，不是两次发布之间的间隔。
     0.2s 的发布节奏看不见亚秒级停顿，所以故障点前后切成无间隔 burst 探测。
  3. 【失去多数派的两种表现】取决于发布方是新连接还是已登记的连接：
     · 新连接 → 首个发布要先在队列状态机里登记，这条 Raft 命令会超时并 NACK；
     · 已登记 → 后续发布只做 pipeline，没有同步应答，于是被挂住等多数派回来。
     两者必须分开测，否则会得出「失去多数派不报错」这种错结论（v2 就差点如此）。

运行（需先起 3 节点集群）：
    cd infra/rabbitmq-cluster
    docker compose up -d
    python3 scripts/failover-timeline.py

实测结果（RabbitMQ 4.3.5，三节点）：见
    experiments/rabbitmq-cluster/raw-failover-timeline.log
"""

import base64
import json
import os
import shutil
import subprocess
import threading
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

EXCHANGE = "gyd.c05.failover.ex"
QUEUE = "gyd.c05.failover.quorum"
RK = "failover"

STEADY = 0.2        # 常规发布节奏
CONVERGE_S = 18
CRED = pika.PlainCredentials("admin", "admin123")

_t0 = time.time()


def log(msg=""):
    print(msg, flush=True)


def ts():
    return time.time() - _t0


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


def quorum_status(queue=QUEUE, timeout=25):
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


def queue_row(timeout=25):
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
                if r.get("name") == QUEUE:
                    return r
        except json.JSONDecodeError:
            continue
    return {}


def ml(v):
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    return [x.strip() for x in str(v or "").strip("[]").split(",") if x.strip()]


def leader_now():
    st = quorum_status()
    return next((n for n, v in st.items() if v["state"] == "leader"), None), st


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
        r = queue_row()
        if r:
            last = len(ml(r.get("online")))
            if last >= n:
                return last
        time.sleep(3)
    return last


def stop_node(ctr):
    """优雅停止：向 PID 1 发 SIGTERM。"""
    sh(DOCKER, "stop", ctr, timeout=90)


def kill_node(ctr):
    """突然失联：SIGKILL，进程没有机会做任何交接。"""
    sh(DOCKER, "kill", ctr, timeout=60)


def start_node(ctr):
    sh(DOCKER, "start", ctr, timeout=90)


def connect(port, name="", timeout_extra=None):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost", port=port, credentials=CRED,
        client_properties={"connection_name": name} if name else None,
        heartbeat=10, blocked_connection_timeout=25, connection_attempts=1))


def declare():
    c = connect(NAME2PORT["gyd-rabbitmq1"], "c05-failover-declarer")
    ch = c.channel()
    for fn in (lambda: ch.queue_delete(QUEUE), lambda: ch.exchange_delete(EXCHANGE)):
        try:
            fn()
        except Exception:
            pass
    ch.exchange_declare(EXCHANGE, "direct", durable=True)
    ch.queue_declare(QUEUE, durable=True, arguments={
        "x-queue-type": "quorum", "x-quorum-initial-group-size": 3})
    ch.queue_bind(QUEUE, EXCHANGE, routing_key=RK)
    c.close()


# ---------------------------------------------------------------- 发布线程

class Publisher(threading.Thread):
    daemon = True

    def __init__(self, conn_name="c05-failover-pub"):
        super().__init__()
        self.conn_name = conn_name
        self.stop_flag = False
        self.entry_ctr = ALL[0]
        self.force_reconnect = False
        self.interval = STEADY
        self.records = []
        self.confirmed = 0
        self.current_start = None      # 正在进行的发布开始时刻（None = 空闲）
        self.lock = threading.Lock()

    def _record(self, t0, t1, status, detail=""):
        with self.lock:
            self.records.append((t0, t1, status, detail))

    def move_to(self, ctr, timeout=40):
        with self.lock:
            self.entry_ctr = ctr
            self.force_reconnect = True
        deadline = time.time() + timeout
        while time.time() < deadline:
            if client_node(self.conn_name) == NODE_NAME[ctr]:
                return True
            time.sleep(0.5)
        return False

    def burst(self, on):
        self.interval = 0.0 if on else STEADY

    def stuck_for(self):
        """若某次发布一直没返回，返回已卡住的秒数；否则 None。"""
        with self.lock:
            s = self.current_start
        return None if s is None else ts() - s

    def run(self):
        body = json.dumps({"pad": "f" * 200}).encode()
        c = ch = None
        while not self.stop_flag:
            if self.force_reconnect:
                try:
                    if c is not None:
                        c.close()
                except Exception:
                    pass
                c = ch = None
                self.force_reconnect = False
            if c is None or c.is_closed or not ch.is_open:
                try:
                    c = connect(NAME2PORT[self.entry_ctr], self.conn_name)
                    ch = c.channel()
                    ch.confirm_delivery()
                except Exception as e:
                    self._record(ts(), ts(), "CONN_FAIL",
                                 f"{type(e).__name__}: {str(e)[:70]}")
                    time.sleep(0.5)
                    c = ch = None
                    continue
            t0 = ts()
            with self.lock:
                self.current_start = t0
            try:
                ch.basic_publish(EXCHANGE, RK, body,
                                 properties=pika.BasicProperties(delivery_mode=2))
                self._record(t0, ts(), "ok")
                self.confirmed += 1
            except Exception as e:
                self._record(t0, ts(), "FAIL", f"{type(e).__name__}: {str(e)[:70]}")
                try:
                    c.close()
                except Exception:
                    pass
                c = ch = None
            finally:
                with self.lock:
                    self.current_start = None
            if self.interval:
                time.sleep(self.interval)
        try:
            if c is not None:
                c.close()
        except Exception:
            pass


def summarize(pub, mark, since, until):
    with pub.lock:
        recs = list(pub.records)
    win = [r for r in recs if since <= r[0] < until]
    ok = [r for r in win if r[2] == "ok"]
    bad = [r for r in win if r[2] != "ok"]
    log(f"  ┌─ {mark}")
    log(f"  │ 尝试 {len(win)} 次，成功确认 {len(ok)}，异常 {len(bad)}")
    if bad:
        kinds = {}
        for r in bad:
            key = f"{r[2]}: {r[3].split(':')[0]}"
            kinds[key] = kinds.get(key, 0) + 1
        for k, v in sorted(kinds.items(), key=lambda x: -x[1]):
            log(f"  │   异常 {k} × {v}")
        log(f"  │   首个异常 t+{bad[0][0]:.2f}s  样本：“{bad[0][3]}”")
        log(f"  │   末个异常 t+{bad[-1][0]:.2f}s")
    if ok:
        durs = sorted(((r[1] - r[0], r) for r in ok), key=lambda x: -x[0])
        med = durs[len(durs) // 2][0]
        log(f"  │ confirm 耗时：中位 {med * 1000:.0f}ms，最慢三条 "
            + "，".join(f"{d * 1000:.0f}ms@t+{r[0]:.2f}s" for d, r in durs[:3]))
        log(f"  │ 成功确认：首条 t+{ok[0][0]:.2f}s，末条 t+{ok[-1][1]:.2f}s")
        if bad:
            first_bad = bad[0][0]
            before = [r for r in ok if r[1] <= first_bad]
            after = [r for r in ok if r[0] > first_bad]
            if before and after:
                log(f"  │ 可确认的失败窗口：t+{before[-1][1]:.2f}s（末次确认成功）"
                    f" → t+{after[0][1]:.2f}s（恢复后首次确认），"
                    f"跨度 {after[0][1] - before[-1][1]:.2f}s")
    else:
        log(f"  │ 该窗口内没有一条成功确认")
    stuck = pub.stuck_for()
    if stuck is not None:
        log(f"  │ ⚠ 结束时线程仍卡在 t+{pub.current_start:.2f}s 开始的那次发布上，"
            f"已挂 {stuck:.1f}s 未返回 —— 这是「被保留」而不是「被拒绝」")
    log(f"  └─")
    return {"attempts": len(win), "ok": len(ok), "bad": len(bad)}


# ---------------------------------------------------------------- 主流程

def main():
    log("=" * 78)
    log("c05 实测 [4][5][6]：三类故障的完整时间线")
    log("=" * 78)

    declare()
    time.sleep(3)

    pub = Publisher()
    pub.start()
    log(f"\n持续发布线程已启动：常规节奏 {STEADY}s，confirm 模式，payload 200B")
    log("每次故障注入前，客户端会被强制搬迁并用管理 API 核实落点。")
    log("选举停顿用「故障点前后切成无间隔 burst 探测」来测，因为 0.2s 的节奏看不见亚秒级停顿。")

    results = {}

    # ---------------- 阶段 0：基线 ----------------
    log(f"\n\n[t+{ts():.2f}s] 【阶段 0】正常状态基线")
    leader, st = leader_now()
    rows = queue_row()
    log(f"  leader = {leader}   term = {st[leader]['term']}   log_index = {st[leader]['log_index']}")
    log(f"  members = {ml(rows.get('members'))}   online = {ml(rows.get('online'))}")
    entry = next(c for c in ALL if NODE_NAME[c] != leader)
    ok = pub.move_to(entry)
    log(f"  客户端搬到 {NODE_NAME[entry]} → 管理 API 核实 = {client_node(pub.conn_name)}  ✓" if ok
        else "  客户端搬迁失败")
    assert ok
    seg = ts()
    time.sleep(2 * CONVERGE_S)
    results["0 正常"] = summarize(pub, "阶段 0：正常状态", seg, ts())

    # ---------------- 阶段 1：Leader 突然失联（SIGKILL） ----------------
    log(f"\n\n[t+{ts():.2f}s] 【阶段 1】Leader 突然失联：docker kill（SIGKILL）")
    leader, st = leader_now()
    victim = NAME2CTR[leader]
    assert pub.entry_ctr != victim, "客户端不能与被杀节点同台，否则测的是接入节点故障"
    log(f"  客户端在 {NODE_NAME[pub.entry_ctr]}；本阶段 {leader} 会被 SIGKILL，"
        f"进程没有机会交接")
    pub.burst(True)
    time.sleep(1.2)
    seg = ts()
    t_kill = ts()
    log(f"  docker kill {victim}   （t+{t_kill:.2f}s）")
    kill_node(victim)
    time.sleep(3.5)
    pub.burst(False)
    log(f"  存活容器 = {wait_alive(2)}")
    log(f"  burst 探测窗口内的原始样本数 = "
        f"{len([r for r in pub.records if r[0] >= seg])}")
    time.sleep(CONVERGE_S)
    leader1, st1 = leader_now()
    if st1:
        log(f"  选举后 leader = {leader1}   term = {st1[leader1]['term']}"
            f"   （选举前 term = {st[leader]['term']}）")
    else:
        log("  quorum_status 在选举窗口内查不到")
    results["1a Leader 被 SIGKILL"] = summarize(
        pub, "阶段 1：Leader 被 SIGKILL（2/3 仍在线）", seg, ts())
    log(f"  本阶段结束时 Leader = {leader1}，term = "
        f"{st1[leader1]['term'] if st1 else '?'}")

    log(f"\n  恢复 {victim}")
    start_node(victim)
    log(f"  存活容器 = {wait_alive(3)}")
    time.sleep(CONVERGE_S)
    log(f"  在线副本 = {wait_online(3, timeout=120)}/3")

    # ---------------- 阶段 1b：Leader 优雅停止（SIGTERM） ----------------
    log(f"\n\n[t+{ts():.2f}s] 【阶段 1b】同一动作改用优雅停止：docker stop（SIGTERM）")
    leader, st = leader_now()
    victim = NAME2CTR[leader]
    if pub.entry_ctr == victim:
        other = next(c for c in ALL if c != victim)
        ok = pub.move_to(other)
        log(f"  客户端搬迁到 {NODE_NAME[other]} → 核实 = {client_node(pub.conn_name)}")
        assert ok
    log(f"  客户端在 {NODE_NAME[pub.entry_ctr]}；本阶段 {leader} 会被 SIGTERM 优雅停止")
    pub.burst(True)
    time.sleep(1.2)
    seg = ts()
    log(f"  docker stop {victim}   （t+{ts():.2f}s）")
    stop_node(victim)
    time.sleep(3.5)
    pub.burst(False)
    log(f"  存活容器 = {wait_alive(2)}")
    time.sleep(CONVERGE_S)
    leader2, st2 = leader_now()
    if st2:
        log(f"  选举后 leader = {leader2}   term = {st2[leader2]['term']}"
            f"   （选举前 term = {st[leader]['term']}）")
    results["1b Leader 被 SIGTERM"] = summarize(
        pub, "阶段 1b：Leader 被 SIGTERM 优雅停止（2/3 仍在线）", seg, ts())

    # ---------------- 阶段 2：恢复并核对追平 ----------------
    log(f"\n\n[t+{ts():.2f}s] 【阶段 2】恢复节点，核对追平")
    start_node(victim)
    log(f"  存活容器 = {wait_alive(3)}")
    time.sleep(CONVERGE_S)
    log(f"  在线副本 = {wait_online(3, timeout=120)}/3")
    st = quorum_status()
    if st:
        logs = {n: v["log_index"] for n, v in st.items()}
        log(f"  各副本 log_index = {logs}  "
            f"{'三份一致' if len(set(logs.values())) == 1 else '尚未一致'}")
    rc, out, err = sh(DOCKER, "exec", "gyd-rabbitmq1", "rabbitmq-queues",
                      "check_if_new_quorum_queue_replicas_have_finished_initial_sync",
                      timeout=40)
    log(f"  初始同步健康检查 → exit={rc}  "
        f"{(out or err).splitlines()[0] if (out or err) else '(无输出)'}")
    log("    （exit=0 且报 no queues with promotable replicas = 没有还在做初始同步的副本）")

    # ---------------- 阶段 3：接入节点故障 ----------------
    log(f"\n\n[t+{ts():.2f}s] 【阶段 3】终止【接入节点】本身（Leader 保持健康）")
    leader, st = leader_now()
    target = next(c for c in ALL if NODE_NAME[c] != leader)
    ok = pub.move_to(target)
    log(f"  当前 leader = {leader}；客户端搬到 {NODE_NAME[target]} → 核实 = "
        f"{client_node(pub.conn_name)}  {'✓' if ok else '✗'}")
    assert ok
    assert pub.entry_ctr != NAME2CTR[leader]
    seg = ts()
    log(f"  docker stop {target}   （t+{ts():.2f}s）  ← Leader {leader} 未受影响")
    stop_node(target)
    log(f"  存活容器 = {wait_alive(2)}")
    time.sleep(2 * CONVERGE_S)
    results["3 接入节点故障"] = summarize(
        pub, f"阶段 3：接入节点 {NODE_NAME[target]} 被终止（Leader 健康）", seg, ts())

    log(f"\n  客户端的连接落在已停节点上，脚本【不替它重连】，先记录它自己挣扎的结果；")
    log(f"  然后模拟客户端自行切换到存活节点。")
    start_node(target)
    log(f"  存活容器 = {wait_alive(3)}")
    live = next(c for c in ALL if c != target)
    seg = ts()
    ok = pub.move_to(live)
    log(f"  客户端接入节点改为 {NODE_NAME[live]} → 核实 = {client_node(pub.conn_name)}")
    assert ok
    time.sleep(2 * CONVERGE_S)
    results["3b 改连存活节点"] = summarize(pub, "阶段 3b：客户端改连存活节点后", seg, ts())

    # ---------------- 阶段 4：失去多数派 ----------------
    log(f"\n\n[t+{ts():.2f}s] 【阶段 4】把三副本压到只剩一个（失去多数派）")
    leader, st = leader_now()
    keep = next(c for c in ALL if NODE_NAME[c] != leader)
    ok = pub.move_to(keep)
    log(f"  当前 leader = {leader}；客户端搬到 {NODE_NAME[keep]} → 核实 = "
        f"{client_node(pub.conn_name)}  {'✓' if ok else '✗'}")
    assert ok
    others = [c for c in ALL if c != keep]

    log("\n  [4a] 先看【已登记的发布方】在失去多数派时的表现")
    before_msgs = queue_row().get("messages")
    seg4a = ts()
    for c in others:
        stop_node(c)
        log(f"    docker stop {c}   （t+{ts():.2f}s）")
        time.sleep(CONVERGE_S)
    log(f"    存活容器 = {wait_alive(1)}")
    time.sleep(CONVERGE_S)
    log(f"    队列视图查询：{'失败/超时' if not queue_row(timeout=20) else '仍可读'}")
    log("    ↑ 失去多数派的不是只有队列：Khepri 元数据存储是另一组 Raft，同样要多数派。")
    results["4a 已登记连接"] = summarize(
        pub, "阶段 4a：失去多数派时，已登记的发布方", seg4a, ts())

    log("\n  [4b] 再看【新建连接的发布方】在同样状态下的表现")
    rc, out, err = sh(DOCKER, "exec", keep, "rabbitmq-diagnostics", "-q",
                      "metadata_store_status", "--formatter", "json", timeout=25)
    txt = (out or err).strip()
    log(f"    元数据存储自身的 Raft 视图（在存活节点 {NODE_NAME[keep]} 上，exit={rc}）：")
    for line in txt.splitlines():
        log(f"      {line}")

    seg4b = ts()
    result4b = {"attempts": 0, "ok": 0, "bad": 0, "err": "", "elapsed": None}
    try:
        c2 = connect(NAME2PORT[keep], "c05-failover-newconn")
        ch2 = c2.channel()
        ch2.confirm_delivery()
        t0 = time.time()
        try:
            ch2.basic_publish(EXCHANGE, RK, json.dumps({"pad": "n"}).encode(),
                              properties=pika.BasicProperties(delivery_mode=2))
            result4b.update(attempts=1, ok=1, elapsed=time.time() - t0)
        except Exception as e:
            result4b.update(attempts=1, bad=1, elapsed=time.time() - t0,
                            err=f"{type(e).__name__}: {str(e)[:120]}")
        finally:
            try:
                c2.close()
            except Exception:
                pass
    except Exception as e:
        result4b.update(err=f"连接失败 {type(e).__name__}: {str(e)[:80]}")
    el = result4b["elapsed"]
    el_txt = "（未走到发布就失败）" if el is None else f"{el:.2f}s"
    log(f"    新连接 + 首条发布：耗时 {el_txt}，"
        f"结果 = {'拿到 confirm' if result4b['ok'] else '未拿到 confirm'}  {result4b['err']}")
    log("    ↑ 新连接的首条发布要先在队列状态机里登记，这条 Raft 命令有同步应答，")
    log("      所以它会以「超时 → reject_publish → basic.nack」的形式明确失败；")
    log("      而 4a 里已登记的信道只做 pipeline，没有同步应答，于是被挂住等多数派回来。")
    log("      官方对这两种结局的原词是 “retained until a majority is restored, or fail with a timeout”。")
    results["4b 新建连接"] = {"attempts": result4b["attempts"], "ok": result4b["ok"],
                             "bad": result4b["bad"]}

    # ---------------- 阶段 5：恢复 ----------------
    log(f"\n\n[t+{ts():.2f}s] 【阶段 5】恢复全部节点")
    for c in others:
        start_node(c)
    log(f"  存活容器 = {wait_alive(3)}")
    time.sleep(CONVERGE_S + 10)
    log(f"  在线副本 = {wait_online(3, timeout=180)}/3")
    st = quorum_status()
    if st:
        logs = {n: v["log_index"] for n, v in st.items()}
        log(f"  各副本 log_index = {logs}  "
            f"{'三份一致' if len(set(logs.values())) == 1 else '尚未一致'}")
    rc, out, err = sh(DOCKER, "exec", "gyd-rabbitmq1", "rabbitmq-queues",
                      "check_if_new_quorum_queue_replicas_have_finished_initial_sync",
                      timeout=40)
    log(f"  初始同步健康检查 → exit={rc}  "
        f"{(out or err).splitlines()[0] if (out or err) else '(无输出)'}")
    seg = ts()
    time.sleep(2 * CONVERGE_S)
    results["5 恢复后"] = summarize(pub, "阶段 5：全部恢复后", seg, ts())

    pub.stop_flag = True
    time.sleep(1.5)

    # ---------------- 汇总 ----------------
    log("\n" + "=" * 78)
    log("汇总")
    log("=" * 78)
    log(f"  {'阶段':<34}{'尝试':>8}{'确认成功':>10}{'异常':>8}")
    for k, v in results.items():
        log(f"  {k:<34}{v['attempts']:>8}{v['ok']:>10}{v['bad']:>8}")

    r = queue_row()
    log(f"\n  恢复后队列：messages = {r.get('messages')}  "
        f"members = {ml(r.get('members'))}  online = {ml(r.get('online'))}")
    log(f"  拿到 confirm 的条数 = {pub.confirmed}；队列中的条数 = {r.get('messages')}")
    log("  两者不必相等：没拿到 confirm 就断连的那些，消息可能已落盘也可能没有 ——")
    log("  confirm 正是用来划这条边界的。这也是消费侧必须幂等的原因（见 C02）。")

    outdir = os.environ.get("C05_TIMELINE_OUT",
                            "experiments/rabbitmq-cluster/raw-failover-full-timeline.tsv")
    with pub.lock:
        recs = list(pub.records)
    with open(outdir, "w") as f:
        f.write("t_start\tt_end\tdur\tstatus\tdetail\n")
        for t0, t1, s, d in recs:
            f.write(f"{t0:.3f}\t{t1:.3f}\t{t1 - t0:.3f}\t{s}\t{d}\n")
    log(f"\n  逐条发布时间线（{len(recs)} 行）已写入 {outdir}")

    # 失败判据：这些阶段必须出结果，否则说明没测到该测的东西
    assert results["3 接入节点故障"]["bad"] >= 1, \
        "接入节点被终止后没有任何失败，说明客户端其实没连在那台上"
    assert results["4b 新建连接"]["ok"] == 0, \
        "失去多数派时新建连接的首条发布竟然拿到 confirm，本阶段无效"


if __name__ == "__main__":
    main()
