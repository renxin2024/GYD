#!/usr/bin/env python3
"""
c05 附：给实验 [7] 阶段 2 的每个观测动作单独计时（耗时归因，不重复结论）。

起因（为什么要单独做这一步）：
  实验 [7] 首轮日志里，【阶段 2】与【阶段 3】两条时间戳之间隔了约 650 秒，
  而其中真正的「发布」动作后来被证明是毫秒级的 —— classic 的 NACK 中位 0.00s，
  quorum 的 confirm 中位 1ms。也就是说那段时间没花在发布上。
  但当时脚本没有给阶段 2 的各个观测动作分别计时，事后无法归因。
  不猜，单独测一遍：在同样的「classic 属主离线」状态下，
  把阶段 2 用到的每个动作各自计时，看时间到底落在哪一步。

这个脚本只做耗时归因：
  · 不复用实验 [7] 的队列，另建一组探针队列，不动那边的产物；
  · 每个动作计时一次（basic_get 这类关键项测两次，看是否稳定）；
  · 测完把节点恢复，并把结论写进本脚本的实测输出。

运行（需先起 3 节点集群）：
    cd infra/rabbitmq-cluster
    docker compose up -d
    python3 scripts/phase2-timing-probe.py

实测结果：见 experiments/rabbitmq-cluster/raw-phase2-timing.log
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

NODES = [("gyd-rabbitmq1", 5673), ("gyd-rabbitmq2", 5674), ("gyd-rabbitmq3", 5675)]
NAME2PORT = dict(NODES)
ALL = [c for c, _ in NODES]
NODE_NAME = {c: "rabbit@" + c[len("gyd-"):] for c, _ in NODES}

EXCHANGE = "gyd.c05.probe.ex"
CLASSIC = "gyd.c05.probe.classic"
QUORUM = "gyd.c05.probe.quorum"
RK_CLASSIC = "probe.classic"
RK_QUORUM = "probe.quorum"

VICTIM, SURVIVOR = "gyd-rabbitmq1", "gyd-rabbitmq2"
CRED = pika.PlainCredentials("admin", "admin123")

_t0 = time.time()


def log(m=""):
    print(m, flush=True)


def ts():
    return time.time() - _t0


def sh(*args, timeout=300):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.returncode, (r.stdout or "").strip(), (r.stderr or "").strip()
    except subprocess.TimeoutExpired:
        return 124, "", f"命令超时（>{timeout}s）"


def api(path):
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


def timed(label, fn):
    """跑一次并计时。异常不抛，类型与文本原样留档 —— 失败形态也是读数。"""
    t0 = time.time()
    try:
        out = fn()
        dt = time.time() - t0
        log(f"  {label:<52} {dt:>7.2f}s   结果={out!r}")
        return dt, out
    except Exception as e:
        dt = time.time() - t0
        log(f"  {label:<52} {dt:>7.2f}s   异常={type(e).__name__}: {str(e)[:90]}")
        return dt, e


def connect(port, name="", heartbeat=10):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost", port=port, credentials=CRED,
        client_properties={"connection_name": name} if name else None,
        heartbeat=heartbeat, blocked_connection_timeout=25, connection_attempts=1))


def queue_row(qname):
    for ctr in ALL:
        rc, out, err = sh(DOCKER, "exec", ctr, "rabbitmqctl", "-q", "list_queues",
                          "name", "type", "leader", "members", "online", "messages",
                          "--formatter", "json", timeout=90)
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
    return None


def quorum_status():
    for ctr in ALL:
        rc, out, err = sh(DOCKER, "exec", ctr, "rabbitmq-queues", "quorum_status",
                          QUORUM, "--formatter", "json", timeout=30)
        txt = (out or err).strip()
        i, j = txt.find("["), txt.rfind("]")
        if i < 0:
            continue
        try:
            return {r["Node Name"]: r["Raft State"] for r in json.loads(txt[i:j + 1])}
        except json.JSONDecodeError:
            continue
    return {}


def declare_and_seed():
    c = connect(NAME2PORT[VICTIM], "c05-probe-declarer")
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
    ch.confirm_delivery()
    body = json.dumps({"pad": "x" * 200}).encode()
    for _ in range(5):
        ch.basic_publish(EXCHANGE, RK_CLASSIC, body,
                         properties=pika.BasicProperties(delivery_mode=2))
        ch.basic_publish(EXCHANGE, RK_QUORUM, body,
                         properties=pika.BasicProperties(delivery_mode=2))
    c.close()


def main():
    log("=" * 78)
    log("c05 附：实验 [7] 阶段 2 各观测动作的耗时归因")
    log(f"环境：RabbitMQ 4.3.5 / 三节点集群 {ALL}")
    log("=" * 78)

    declare_and_seed()
    time.sleep(2)
    log(f"\n[t+{ts():.2f}s] 探针拓扑已声明并各投 5 条（{EXCHANGE}）")

    log("\n【故障前】基线耗时")
    timed("rabbitmq-queues quorum_status（故障前）", quorum_status)
    timed("管理 API GET /api/queues/%2F/<classic>（故障前）",
          lambda: api("queues/%2F/" + urllib.parse.quote(CLASSIC, safe="")).get("state"))
    timed("rabbitmqctl list_queues（故障前，取 classic 行）",
          lambda: bool(queue_row(CLASSIC)))

    log(f"\n[t+{ts():.2f}s] 【注入故障】SIGKILL {NODE_NAME[VICTIM]}"
        "（classic 属主离线，quorum 仍余 2/3）")
    sh(DOCKER, "kill", VICTIM, timeout=60)
    time.sleep(3)
    alive = [c for c in ALL
             if sh(DOCKER, "exec", c, "rabbitmq-diagnostics", "-q", "ping",
                   timeout=15)[0] == 0]
    log(f"  存活容器 = {alive}")

    log("\n【故障中】逐个动作计时 —— 看时间到底花在哪一步")
    timed("管理 API GET /api/queues/%2F/<classic>",
          lambda: api("queues/%2F/" + urllib.parse.quote(CLASSIC, safe="")).get("state"))
    timed("管理 API GET /api/queues/%2F/<quorum>",
          lambda: api("queues/%2F/" + urllib.parse.quote(QUORUM, safe="")).get("leader"))
    timed("管理 API GET /api/connections",
          lambda: len(api("connections")))
    timed("rabbitmqctl list_queues（含 classic 行）", lambda: bool(queue_row(CLASSIC)))
    timed("rabbitmq-queues quorum_status", quorum_status)

    def get_classic():
        c = connect(NAME2PORT[SURVIVOR], "c05-probe-get-classic")
        ch = c.channel()
        try:
            return ch.basic_get(CLASSIC, auto_ack=True)
        finally:
            try:
                c.close()
            except Exception:
                pass

    def get_quorum():
        c = connect(NAME2PORT[SURVIVOR], "c05-probe-get-quorum")
        ch = c.channel()
        try:
            m = ch.basic_get(QUORUM, auto_ack=True)
            return m is not None
        finally:
            try:
                c.close()
            except Exception:
                pass

    timed("basic_get(classic) 【第 1 次】", get_classic)
    timed("basic_get(classic) 【第 2 次】", get_classic)
    timed("basic_get(classic) 【第 3 次】", get_classic)
    timed("basic_get(quorum)", get_quorum)

    def pub_classic():
        c = connect(NAME2PORT[SURVIVOR], "c05-probe-pub-classic")
        ch = c.channel()
        ch.confirm_delivery()
        try:
            ch.basic_publish(EXCHANGE, RK_CLASSIC,
                             json.dumps({"pad": "x" * 200}).encode(),
                             properties=pika.BasicProperties(delivery_mode=2))
            return "confirm 成功"
        finally:
            try:
                c.close()
            except Exception:
                pass

    # 多采几个样本：这个动作的耗时在两次完整运行之间差了三个数量级
    # （一轮毫秒级，另一轮按 20 条 / 577s 折算约 29s 一条），单次取值没有代表性。
    for i in range(5):
        timed(f"发布 1 条到 classic（confirm 模式）【第 {i + 1} 次】", pub_classic)

    log(f"\n[t+{ts():.2f}s] 【恢复】docker start {NODE_NAME[VICTIM]}")
    sh(DOCKER, "start", VICTIM, timeout=90)
    deadline = time.time() + 180
    while time.time() < deadline:
        rc, _, _ = sh(DOCKER, "exec", VICTIM, "rabbitmq-diagnostics", "-q", "ping",
                      timeout=15)
        if rc == 0:
            break
        time.sleep(3)
    log(f"  恢复用时 {time.time() - (deadline - 180):.1f}s")
    time.sleep(5)

    log("\n【恢复后】复核探针队列")
    r = queue_row(CLASSIC)
    log(f"  classic messages = {r.get('messages') if r else '读不到'}")
    r = queue_row(QUORUM)
    log(f"  quorum  messages = {r.get('messages') if r else '读不到'}")

    log("\n" + "=" * 78)
    log("小结：上表中耗时明显偏大的那一行，就是实验 [7] 阶段 2 时间去向的答案")
    log("=" * 78)


if __name__ == "__main__":
    main()
