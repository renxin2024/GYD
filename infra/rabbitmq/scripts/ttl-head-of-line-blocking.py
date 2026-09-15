#!/usr/bin/env python3
"""
实测：per-message TTL（AMQP `expiration` 属性）在单队列中的「队头阻塞」。

对应文章：GYD 第 2 篇《消息队列的可靠投递与可靠消费》第五节
（解释「TTL 延迟重试为什么只能做固定间隔，做不了递增退避」）。

要回答的问题：
    同一条队列里，能否给每条消息设置各自不同的延迟？
    —— 直觉是「谁先过期谁先走」，实测是「队头说了算」。

依赖：
    pip install pika

运行（需先起单节点 RabbitMQ）：
    cd infra/rabbitmq && docker compose up -d
    python3 infra/rabbitmq/scripts/ttl-head-of-line-blocking.py

实测结果（每次运行有毫秒级抖动，量级稳定）：
    实验 A  入队 6000ms → 1000ms → 1000ms
            三条全部在 +6.0s 左右才死信 —— 后两条被队头那条卡了约 5 秒
    实验 B  入队 1000ms → 1000ms → 6000ms
            前两条 +1.0s 左右死信，第三条 +6.0s 左右

结论：
    过期检查只在队头进行。队头消息没过期，后面已经过期的消息也不会被
    丢弃 / 死信。所以「单队列 + per-message TTL」做不了按消息各自的延迟；
    要做递增退避，只能每级一个队列（或延迟插件）。
"""

import json
import threading
import time

import pika

HOST, PORT = "localhost", 5672
CRED = pika.PlainCredentials("admin", "admin123")

T0 = [0.0]  # 投递起始时刻，消费者线程据此换算相对时间


def connect():
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=HOST, port=PORT, credentials=CRED,
            heartbeat=30, blocked_connection_timeout=30,
        )
    )


def run_experiment(tag, plan, hold_s=9.0):
    """plan: [(label, ttl_ms), ...]，列表顺序即入队顺序。"""
    # 资源名带唯一后缀，保证脚本可重复运行（上一次中断留下的残留不会冲突）
    uniq = f"{int(time.time() * 1000)}"
    base = f"gyd.c02.ttl.hol.{tag}.{uniq}"
    ex = f"{base}.ex"
    dlx = f"{base}.dlx"
    q = base
    dlq = f"{base}.dlq"

    # ---- 建拓扑 ----
    # 注意：RabbitMQ 4.3 已弃用「非持久化的非排他队列」，这里一律用持久化队列，
    # 实验结束再显式删除。
    conn = connect()
    ch = conn.channel()
    ch.exchange_declare(ex, "direct", durable=True)
    ch.exchange_declare(dlx, "direct", durable=True)
    ch.queue_declare(dlq, durable=True)
    ch.queue_bind(dlq, dlx, routing_key="dead")
    ch.queue_declare(
        q, durable=True,
        arguments={
            "x-dead-letter-exchange": dlx,
            "x-dead-letter-routing-key": "dead",
        },
    )
    ch.queue_bind(q, ex, routing_key="k")
    ch.queue_purge(q)
    ch.queue_purge(dlq)
    conn.close()

    # ---- DLQ 侧起消费者，记录到达时刻 ----
    events = []
    conn_cons = connect()
    ch_cons = conn_cons.channel()
    ch_cons.basic_qos(prefetch_count=10)

    def on_dead(_ch, method, _props, body):
        dt = time.monotonic() - T0[0]
        info = json.loads(body)
        events.append((dt, info["label"], info["ttl"]))
        print(f"    [+{dt:6.3f}s]  DLQ 收到  {info['label']}  (ttl={info['ttl']}ms)")
        _ch.basic_ack(method.delivery_tag)

    ch_cons.basic_consume(dlq, on_dead, auto_ack=False)
    th = threading.Thread(target=ch_cons.start_consuming, daemon=True)
    th.start()
    time.sleep(0.5)  # 等消费者就绪，再开始计时

    # ---- 按顺序投递 ----
    print(f"\n  === 实验 {tag} ===  入队顺序: "
          + " -> ".join(f"{l}(ttl={t}ms)" for l, t in plan))
    conn_pub = connect()
    ch_pub = conn_pub.channel()
    T0[0] = time.monotonic()
    for label, ttl in plan:
        body = json.dumps({"label": label, "ttl": ttl}).encode()
        ch_pub.basic_publish(
            ex, "k", body,
            properties=pika.BasicProperties(expiration=str(ttl), delivery_mode=2),
        )
        print(f"    [入队]      {label}  (ttl={ttl}ms)")
    conn_pub.close()

    time.sleep(hold_s)
    conn_cons.add_callback_threadsafe(ch_cons.stop_consuming)
    th.join(timeout=3)
    try:
        conn_cons.close()
    except Exception:
        pass

    # ---- 清理 ----
    conn = connect()
    ch = conn.channel()
    ch.queue_delete(q)
    ch.queue_delete(dlq)
    ch.exchange_delete(ex)
    ch.exchange_delete(dlx)
    conn.close()
    return events


def main():
    print("=" * 68)
    print("实测：per-message TTL 的队头阻塞（单队列，无消费者）")
    print("=" * 68)

    a = run_experiment("A", [("A-long", 6000), ("A-short1", 1000), ("A-short2", 1000)])
    b = run_experiment("B", [("B-short1", 1000), ("B-short2", 1000), ("B-long", 6000)])

    print("\n" + "=" * 68)
    print("汇总")
    print("=" * 68)
    print("实验 A  入队 A-long(6000) -> A-short1(1000) -> A-short2(1000)")
    for dt, label, ttl in a:
        print(f"        {label:<10} ttl={ttl:>5}ms   实际死信于 +{dt:.3f}s")
    print("实验 B  入队 B-short1(1000) -> B-short2(1000) -> B-long(6000)")
    for dt, label, ttl in b:
        print(f"        {label:<10} ttl={ttl:>5}ms   实际死信于 +{dt:.3f}s")


if __name__ == "__main__":
    main()
