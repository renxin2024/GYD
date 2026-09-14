package cn.renxinblog.gyd.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 第六节 TTL 部分的实测。
 *
 * <ol>
 *   <li>队列级 {@code x-message-ttl}：到期的消息保证不投递，也确实会消失</li>
 *   <li>队头规则：per-message TTL 过期的消息，要排到队头才真被丢弃</li>
 *   <li>队列级与 per-message 并存时取较小值</li>
 *   <li>requeue 之后到期时间有没有被重新计时</li>
 * </ol>
 */
final class TtlDemos {

    private TtlDemos() {
    }

    static void run() throws Exception {
        DemoRunner.head("ttl");
        DemoRunner.say("reset", "清理上一轮遗留的本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());

        String q1 = DemoRunner.P + "ttl.queue-level";
        String q2 = DemoRunner.P + "ttl.head-of-line";
        String q3 = DemoRunner.P + "ttl.both";
        String q4 = DemoRunner.P + "ttl.requeue";
        String[] queues = {q1, q2, q3, q4};

        Connection conn = DemoRunner.newConnection();
        try {
            Channel ch = conn.createChannel();

            // 1) 队列级 TTL
            long t0 = System.currentTimeMillis();
            DemoRunner.say("ttl", "--- 1) 队列级 x-message-ttl=3000 ---");
            DemoRunner.declareQueue(ch, q1, DemoRunner.args("x-message-ttl", 3000));
            for (int i = 1; i <= 3; i++) {
                DemoRunner.publish(ch, q1, "ttl-" + i);
            }
            DemoRunner.sleep(400);
            at("ttl", t0, "刚发布完");
            DemoRunner.state("ttl", ch, q1);

            DemoRunner.sleep(1500);
            at("ttl", t0, "还没到期");
            DemoRunner.state("ttl", ch, q1);

            DemoRunner.sleep(2200);
            at("ttl", t0, "已过 3 秒");
            DemoRunner.state("ttl", ch, q1);

            List<String> afterExpiry = DemoRunner.drain(ch, q1, true, null, 1, 1500);
            at("ttl", t0, "此时再起消费者 → 读到 %d 条（到期消息保证不投递）", afterExpiry.size());

            // 2) 队头规则
            DemoRunner.say("ttl", "--- 2) 队头规则：过期不等于立刻释放 ---");
            long t1 = System.currentTimeMillis();
            DemoRunner.declareQueue(ch, q2, null);
            DemoRunner.publishWithExpiration(ch, q2, "A(per-message ttl=60000)", "60000");
            DemoRunner.publishWithExpiration(ch, q2, "B(per-message ttl=2000)", "2000");
            DemoRunner.sleep(3000);
            at("ttl", t1, "先发的 A 是 60 秒；后发的 B 只有 2 秒，B 早该过期了");
            DemoRunner.state("ttl", ch, q2);

            List<String> gotOne = DemoRunner.drain(ch, q2, true, null, 1, 2500);
            at("ttl", t1, "排到队头并消费：%s", gotOne);
            DemoRunner.state("ttl", ch, q2);
            int q2Depth = DemoRunner.depth(ch, q2);
            at("ttl", t1, "消费后队列深度 = %d，消费者只拿到 %d 条 → %s",
                    q2Depth, gotOne.size(),
                    (q2Depth == 0 && gotOne.size() == 1)
                            ? "后发的那条 B 到队头时被直接丢弃，消费者拿不到它"
                            : "结果与「过期即丢弃」不符，按实测记录");

            // 3) 两者并存取较小值
            DemoRunner.say("ttl", "--- 3) 队列级与 per-message 并存 ---");
            long t2 = System.currentTimeMillis();
            DemoRunner.declareQueue(ch, q3, DemoRunner.args("x-message-ttl", 8000));
            DemoRunner.publishWithExpiration(ch, q3, "m1(per-message 1500)", "1500");
            DemoRunner.publishWithExpiration(ch, q3, "m2(per-message 60000)", "60000");
            DemoRunner.sleep(3000);
            DemoRunner.state("ttl", ch, q3);
            int d3a = DemoRunner.depth(ch, q3);
            at("ttl", t2, "3 秒时深度 = %d（m1 的 1.5 秒、m2 的 60 秒，队列级 8 秒在两者之上/之下各压一次）", d3a);
            DemoRunner.sleep(6000);
            DemoRunner.state("ttl", ch, q3);
            int d3b = DemoRunner.depth(ch, q3);
            at("ttl", t2, "9 秒时深度 = %d → %s", d3b,
                    (d3a == 1 && d3b == 0)
                            ? "生效的 TTL 是「队列级与 per-message 里较小的那个」"
                            : "与「取较小值」不符，按实测记录");

            // 4) requeue 之后到期时间
            //
            // 这一段的关键是**让两种假设的到期时刻拉开距离**。上一版把 requeue 放在投递后立刻做，
            // 那时「不重新计时」与「重新计时」只差 0.3 秒，读到的「已消失」两种假设都能解释——
            // 等于没测。改成：TTL 12 秒，先让另一条连接把消息取走按住 6 秒，再关那条连接让它退回。
            //   不重新计时 → 按首次发布时刻算，t≈12s 到期
            //   重新计时   → 从退回时刻算，t≈18s 到期
            // 于是「t≈13.8s 还在不在」这个读数就能把两者分开。
            DemoRunner.say("ttl", "--- 4) requeue 之后到期时间有没有重新计时 ---");
            long t3 = System.currentTimeMillis();
            DemoRunner.declareQueue(ch, q4, null);
            DemoRunner.publishWithExpiration(ch, q4, "r(per-message 12000)", "12000");
            DemoRunner.sleep(300);

            Connection conn2 = DemoRunner.newConnection();
            final CountDownLatch one = new CountDownLatch(1);
            Channel chB = conn2.createChannel();
            chB.basicQos(1);
            chB.basicConsume(q4, false, (ct, delivery) -> one.countDown(), ct -> { });
            one.await(3, TimeUnit.SECONDS);
            at("ttl", t3, "消息已被另一条连接取走并按住不 ack（它的 TTL 12 秒从发布时刻起算）");

            DemoRunner.sleep(6000);
            DemoRunner.closeQuietly(conn2);
            at("ttl", t3, "按住 6 秒后关掉那条连接 → 消息退回队列；若 TTL 被重新计时，它会活到 t≈18s");

            DemoRunner.sleep(4000);
            DemoRunner.state("ttl", ch, q4);
            int d4a = DemoRunner.depth(ch, q4);
            at("ttl", t3, "t≈10.3s 深度 = %d（两种假设下都该还在）", d4a);

            DemoRunner.sleep(3500);
            DemoRunner.state("ttl", ch, q4);
            int d4b = DemoRunner.depth(ch, q4);
            at("ttl", t3, "t≈13.8s 深度 = %d → %s", d4b,
                    d4b == 0
                            ? "requeue 没有重新计时：它按首次发布的时刻到期（12 秒），而不是按退回时刻"
                            : "requeue 重新计时了：它活过了首次发布后 12 秒，说明退回时重新起算");

            DemoRunner.cleanup(ch, queues, new String[]{});
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }

    private static void at(String tag, long t0, String format, Object... args) {
        DemoRunner.say(tag, "t=%5.1fs  " + format, prepend((System.currentTimeMillis() - t0) / 1000.0, args));
    }

    private static Object[] prepend(Object first, Object[] rest) {
        Object[] all = new Object[rest.length + 1];
        all[0] = first;
        System.arraycopy(rest, 0, all, 1, rest.length);
        return all;
    }
}
