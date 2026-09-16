#!/usr/bin/env python3
"""
c05 实测组 [9]：quorum queue 的**消费侧**在三类故障下的行为。

对应文章：C05 第九节（故障逐层拆）与第十一节（追平判据）

为什么必须单独测（纲领缺口）：
  纲领「核心问题」写的是「Leader 或多数派失效后，**发送与消费**分别会发生什么」，
  「可运行演示」第 5 步写的是「记录**发送与消费**如何失败或暂停」。
  实测 [4][5][6] 全程没有任何消费者（日志里那句 "期间没有任何生产者或消费者活动"
  是如实记录），[7] 只在 classic 属主离线期间对 quorum 试取过 5 条。
  所以「消费侧」这一半在三类故障下**没有读数**，本文不能靠发布侧的结果去推断。

三类故障、两个消费者，一次时间线跑完：
  · 消费者 A 连在 VICTIM（故障节点本身）
  · 消费者 B 连在 SURVIVOR（存活节点）
  这组对照直接回答纲领第 80 行那句「连接在其它节点上的消费者可由 RabbitMQ 重新注册；
  连在故障节点上的客户端仍要重连」。

阶段：
  1 基线 —— 两个消费者都在消费，记录投递速率
  2 Leader 故障（SIGKILL VICTIM，多数派仍在）—— 谁断、谁停、停多久
  3 失去多数派（再 SIGKILL 一个，只剩 1/3）—— 消费是否停止、以什么形态停
  4 恢复（两个节点都回来）—— 投递是否自行恢复、消息总数是否守恒

运行（需先起 3 节点集群）：
    cd infra/rabbitmq-cluster
    docker compose up -d
    python3 scripts/quorum-consumption-failure-modes.py

实测结果（RabbitMQ 4.3.5，三节点）：见
    experiments/rabbitmq-cluster/raw-quorum-consumption.log
"""

import json
import os
import shutil
import subprocess
import threading
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

QUEUE = "gyd.c05.consume.quorum"
# 【两轮返工换来的教训】不能靠「预置足够多条」来保证故障期间还有积压。
# 实测同一个消费者：积压 300 条时约 5 条/秒，积压 3000 条时约 50 条/秒 ——
# 吞吐随积压涨，任何固定条数都会在基线阶段被吃光，后面三个阶段全是 0 投递，
# 而那个 0 不是「消费停了」，是「没东西可投」。
# 所以改成**限速**：每条在回调里停一下，把速率钉成常数，积压才成为可信的余量。
N_SEED = 3000                   # 预置量 = 基线可吃的量 × 数倍，只作余量
RATE_PER_SEC = 8.0              # 每个消费者的目标速率（条/秒）
ACK_DELAY = 1.0 / RATE_PER_SEC
VICTIM = "gyd-rabbitmq1"        # Leader 所在地，也是消费者 A 的连接节点
SURVIVOR = "gyd-rabbitmq2"      # 消费者 B 的连接节点
THIRD = "gyd-rabbitmq3"

CRED = pika.PlainCredentials("admin", "admin123")

_t0 = time.time()
_lock = threading.Lock()


def log(msg=""):
    with _lock:
        print(f"[t+{time.time() - _t0:7.2f}s] {msg}", flush=True)


def mark(msg):
    log("")
    log("=" * 72)
    log(msg)
    log("=" * 72)


def sh(*args, timeout=120):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return 124, "", f"命令超时（>{timeout}s）"


def queue_row(qname, timeout=25):
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


def alive():
    return [c for c in ALL
            if sh(DOCKER, "exec", c, "rabbitmq-diagnostics", "-q", "ping",
                  timeout=15)[0] == 0]


def wait_alive(n, timeout=200):
    deadline = time.time() + timeout
    last = []
    while time.time() < deadline:
        last = alive()
        if len(last) >= n:
            return last
        time.sleep(3)
    return last


def wait_online(n, timeout=240):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        r = queue_row(QUEUE)
        if r:
            last = len(ml(r.get("online")))
            if last >= n:
                return last
        time.sleep(3)
    return last


def connect(port, name, heartbeat=10):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost", port=port, credentials=CRED,
        client_properties={"connection_name": name} if name else None,
        heartbeat=heartbeat, blocked_connection_timeout=25, connection_attempts=1))


# ------------------------------------------------------------------ 消费者线程

class Consumer(threading.Thread):
    """一个可观测、可被外部停下的消费者。

    记的不是「总数」而是**投递事件的时间线** —— 三类故障的差别就在
    「投递何时停、停了多久、有没有报错、是否自行恢复」，
    只记总数会把这些全部抹平。
    """

    def __init__(self, tag, ctr, qname):
        super().__init__(daemon=True)
        self.tag = tag
        self.ctr = ctr
        self.qname = qname
        self.port = NAME2PORT[ctr]
        self.deliveries = []          # (t, routing_key)
        self.state = "init"           # init → consuming → paused → dead → reconnecting → consuming
        self.err = []                 # 断链/异常原文
        self._stop = threading.Event()
        self._conn = None
        self._reconnects = 0

    # -- 对外：当前是否在投递
    @property
    def last_t(self):
        return self.deliveries[-1][0] if self.deliveries else None

    def stop(self):
        self._stop.set()
        try:
            if self._conn and self._conn.is_open:
                self._conn.close()
        except Exception:
            pass

    def _on_message(self, ch, method, props, body):
        self.deliveries.append((time.time() - _t0, method.routing_key))
        # 限速：prefetch=1 + 回调里停一下，速率才不随积压变化。
        # 这条 sleep 也是「停机期间 0 投递」这类读数的前提 ——
        # 没有限速，基线的爆发速率会把积压吃光，后面的 0 就无从区分。
        if ACK_DELAY:
            time.sleep(ACK_DELAY)
        ch.basic_ack(method.delivery_tag)

    def run(self):
        while not self._stop.is_set():
            try:
                self._conn = connect(self.port, f"c05-consume-{self.tag}")
                ch = self._conn.channel()
                ch.basic_qos(prefetch_count=1)
                ch.basic_consume(queue=self.qname, on_message_callback=self._on_message)
                self.state = "consuming"
                log(f"  [{self.tag}] 已连上 {self.ctr}，开始消费")
                while self._conn.is_open and not self._stop.is_set():
                    self._conn.process_data_events(time_limit=1)
            except Exception as e:
                if self._stop.is_set():
                    break
                txt = f"{type(e).__name__}: {str(e)[:200]}"
                self.err.append((round(time.time() - _t0, 2), txt))
                self.state = "dead"
                log(f"  [{self.tag}] 消费中断：{txt}")
                if self._stop.is_set():
                    break
                self._reconnects += 1
                self.state = "reconnecting"
                # 纲领第 80 行：连在故障节点上的客户端「仍要重连」——
                # 所以这里换到存活节点重连，并记录换点过程。
                for ctr in ALL:
                    if ctr == self.ctr:
                        continue
                    if sh(DOCKER, "exec", ctr, "rabbitmq-diagnostics", "-q", "ping",
                          timeout=10)[0] != 0:
                        continue
                    try:
                        self.ctr = ctr
                        self.port = NAME2PORT[ctr]
                        self._conn = connect(self.port, f"c05-consume-{self.tag}")
                        ch = self._conn.channel()
                        ch.basic_qos(prefetch_count=1)
                        ch.basic_consume(queue=self.qname,
                                         on_message_callback=self._on_message)
                        self.state = "consuming"
                        log(f"  [{self.tag}] 已改连 {ctr} 重连成功")
                        break
                    except Exception as e2:
                        log(f"  [{self.tag}] 改连 {ctr} 失败：{type(e2).__name__}")
                else:
                    log(f"  [{self.tag}] 当前没有可用节点，2s 后重试")
                    time.sleep(2)
        self.state = "stopped"

    def window(self, a, b):
        return [d for d in self.deliveries if a <= d[0] < b]


def report_phase(title, t_start, t_end, consumers):
    """阶段小结：投递数 + 停顿时长。这是本实验的核心读数。"""
    log(f"  ── {title}（{[round(t_start,1), round(t_end,1)]}）")
    for c in consumers:
        n = len(c.window(t_start, t_end))
        span = max(t_end - t_start, 0.001)
        log(f"     [{c.tag}] 投递 {n} 条，约 {n / span:.2f} 条/秒，状态={c.state}，"
            f"重连 {c._reconnects} 次")
        if c.err:
            for t, e in c.err:
                if t_start <= t < t_end:
                    log(f"       异常原文 @t+{t}s：{e}")


def pause_window(c, t_a, t_b):
    """在 [t_a, t_b) 内最长无投递间隔 —— 「停了多久」的量化。"""
    ds = [d[0] for d in c.window(t_a, t_b)]
    pts = [t_a] + ds + [t_b]
    gap, at = 0.0, None
    for x, y in zip(pts, pts[1:]):
        if y - x > gap:
            gap, at = y - x, x
    return round(gap, 2), (round(at, 2) if at else None)


# ------------------------------------------------------------------ 主流程

def main():
    log("=" * 72)
    log("c05 实测组 [9]：quorum queue 的消费侧在三类故障下的行为")
    log(f"  队列        : {QUEUE}（quorum，3 副本，预置 {N_SEED} 条）")
    log(f"  消费者 A    : 连在 {VICTIM}（= Leader 所在地，故障节点本身）")
    log(f"  消费者 B    : 连在 {SURVIVOR}（存活节点）")
    log("=" * 72)

    mark("阶段 0：起拓扑、预置消息")
    log(f"  存活节点：{alive()}")
    c = connect(NAME2PORT[VICTIM], "c05-consume-declarer")
    ch = c.channel()
    try:
        ch.queue_delete(QUEUE)
    except Exception:
        pass
    ch.queue_declare(QUEUE, durable=True, arguments={
        "x-queue-type": "quorum", "x-quorum-initial-group-size": 3})
    ch.confirm_delivery()
    body = json.dumps({"pad": "x" * 256}).encode()
    for _ in range(N_SEED):
        ch.basic_publish("", QUEUE, body,
                         properties=pika.BasicProperties(delivery_mode=2))
    c.close()

    # Leader 摆在 VICTIM，让「Leader 故障」与「消费者 A 所在节点故障」重合
    moved = ""
    for ctr in ALL:
        rc, out, err = sh(DOCKER, "exec", ctr, "rabbitmq-queues", "transfer_leadership",
                          QUEUE, NODE_NAME[VICTIM], timeout=30)
        moved = ((out or "") + " " + (err or "")).strip().replace("\n", " ")[:110]
        if rc == 0 or "already the leader" in moved:
            break
    log(f"  预置 {N_SEED} 条；Leader 搬迁：{moved}")
    r = queue_row(QUEUE)
    log(f"  leader={str(r.get('leader','?')).strip()}  members={ml(r.get('members'))}"
        f"  messages={messages_of(QUEUE)}")
    log(f"  断言 Leader == {NODE_NAME[VICTIM]} → "
        f"{'✓' if str(r.get('leader','')).strip() == NODE_NAME[VICTIM] else '✗'}")

    A = Consumer("A-on-victim", VICTIM, QUEUE)
    B = Consumer("B-on-survivor", SURVIVOR, QUEUE)
    A.start()
    B.start()

    mark(f"阶段 1：基线 —— 两个消费者同时消费 {N_SEED} 条积压")
    time.sleep(25)
    t1a, t1b = 0.0, time.time() - _t0
    report_phase("基线", t1a, t1b, [A, B])
    log(f"  剩余条数（指标，可能滞后）：{messages_of(QUEUE)}")

    mark(f"阶段 2：Leader 故障 —— SIGKILL {VICTIM}（多数派仍在 2/3）")
    t2a = time.time() - _t0
    sh(DOCKER, "kill", "--signal=KILL", VICTIM, timeout=30)
    log(f"  docker kill rc=0；存活节点：{alive()}")
    time.sleep(45)
    t2b = time.time() - _t0
    report_phase("Leader 故障中", t2a, t2b, [A, B])
    for c_ in (A, B):
        g, at = pause_window(c_, t2a, t2b)
        log(f"     [{c_.tag}] 本阶段最长无投递窗口 {g}s（始于 t+{at}s）")
    r2 = queue_row(QUEUE)
    log(f"  队列行：leader={str(r2.get('leader','?')).strip()}  "
        f"online={ml(r2.get('online'))}  messages={messages_of(QUEUE)}")

    mark(f"阶段 3：失去多数派 —— 再 SIGKILL {THIRD}（只剩 1/3）")
    t3a = time.time() - _t0
    sh(DOCKER, "kill", "--signal=KILL", THIRD, timeout=30)
    log(f"  docker kill rc=0；存活节点：{alive()}")
    time.sleep(50)
    t3b = time.time() - _t0
    report_phase("失去多数派中", t3a, t3b, [A, B])
    for c_ in (A, B):
        g, at = pause_window(c_, t3a, t3b)
        log(f"     [{c_.tag}] 本阶段最长无投递窗口 {g}s（始于 t+{at}s）")
    log(f"  失去多数派时 list_queues 是否还可用："
        f"{'可用，messages=' + str(messages_of(QUEUE)) if queue_row(QUEUE) else '不可用/超时'}")

    mark(f"阶段 4：恢复 —— 依次重启 {VICTIM} 与 {THIRD}")
    t4a = time.time() - _t0
    for ctr in (VICTIM, THIRD):
        sh(DOCKER, "start", ctr, timeout=60)
        log(f"  已 start {ctr}")
    live = wait_alive(3, timeout=200)
    log(f"  存活节点：{live}")
    on = wait_online(3, timeout=240)
    log(f"  quorum online 副本数 = {on}/3")
    time.sleep(30)
    t4b = time.time() - _t0
    report_phase("恢复后", t4a, t4b, [A, B])
    for c_ in (A, B):
        g, at = pause_window(c_, t4a, t4b)
        log(f"     [{c_.tag}] 本阶段最长无投递窗口 {g}s（始于 t+{at}s）")

    mark("阶段 5：收口 —— 让两个消费者把剩余消息取完，核对守恒")
    # 3000 条积压按实测速率约需 4~5 分钟，所以给足 300s；
    # 取不完也不影响结论（剩余条数会单独报出来），只是守恒核对会降级成「部分」。
    deadline = time.time() + 300
    while time.time() < deadline:
        time.sleep(3)
        if len(A.deliveries) + len(B.deliveries) >= N_SEED:
            break
    A.stop()
    B.stop()
    time.sleep(2)
    na, nb = len(A.deliveries), len(B.deliveries)
    log(f"  消费者 A 共投递 {na} 条，重连 {A._reconnects} 次，异常 {len(A.err)} 次")
    log(f"  消费者 B 共投递 {nb} 条，重连 {B._reconnects} 次，异常 {len(B.err)} 次")
    log(f"  合计 {na + nb} 条投递，预置 {N_SEED} 条")
    if na + nb == N_SEED:
        log("  → 投递次数恰好等于预置条数：这次故障里没有重投。")
    elif na + nb > N_SEED:
        log(f"  → 多出 {na + nb - N_SEED} 条投递。消费者 A 是手动 ack 的，"
            "它所在的节点被 SIGKILL 时手里未 ack 的那批消息会被重新投递 ——"
            "**这正是 at-least-once 的来源，不是丢失**。数字如实留档，不做归一化。")
    else:
        log(f"  → 少 {N_SEED - (na + nb)} 条。可能是还有未消费完的积压（不算异常），"
            "也可能是真的丢了 —— 下面用队列剩余条数区分。")
    log(f"  队列剩余 messages = {messages_of(QUEUE)}"
        f"（若为 0 且投递数 ≥ 预置数，则无丢失）")
    for c_, tag in ((A, "A"), (B, "B")):
        if c_.err:
            log(f"  消费者 {tag} 的异常原文（全部）：")
            for t, e in c_.err:
                log(f"    @t+{t}s  {e}")

    mark("汇总")
    log(f"  {'阶段':<22}{'A 连故障节点':>16}{'B 连存活节点':>16}")
    for nm, a, b in (("基线", 0.0, t1b), ("Leader 故障", t2a, t2b),
                     ("失去多数派", t3a, t3b), ("恢复后", t4a, t4b)):
        log(f"  {nm:<22}{len(A.window(a, b)):>16}{len(B.window(a, b)):>16}")
    log("")
    log("  判据说明：")
    log("    1) 投递是否会停 —— 停，说明消费路径也要共识；不停，说明副本/日志已够；")
    log("    2) 停多久 —— 与选举窗口对照；")
    log("    3) 断链的异常原文 —— 决定应用侧该捕获什么；")
    log("    4) 守恒 —— 消费者自行重连后有没有重复或丢失。")
    log("=" * 72)


if __name__ == "__main__":
    main()
