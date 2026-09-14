package cn.renxinblog.gyd.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 第六节死信部分的实测。
 *
 * <p>四个触发点里，前两个（nack、per-message TTL 到期）是官方文档明确写了的；第三个
 * （队列级 {@code x-message-ttl} 到期）是台账 14 的必闭项——官方 dlx 页只写了
 * 「per-message TTL」，而 ttl 页的「Message TTL and dead lettering」同时覆盖队列级配置，
 * 只能实测给结论。第四个（整队被 {@code x-expires} 删掉）是官方写明的例外。
 */
final class DlxDemos {

    private static final String EX_DLX = DemoRunner.P + "dlx";
    private static final String Q_DLQ = DemoRunner.P + "dlq";

    private static final String Q_NACK = DemoRunner.P + "dlx.main.nack";
    private static final String Q_MSG_TTL = DemoRunner.P + "dlx.main.msgttl";
    private static final String Q_QUEUE_TTL = DemoRunner.P + "dlx.main.qttl";
    private static final String Q_EXPIRES = DemoRunner.P + "dlx.main.qexp";

    private DlxDemos() {
    }

    static void run() throws Exception {
        DemoRunner.head("dlx");
        DemoRunner.say("reset", "清理上一轮遗留的本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());

        String[] queues = {Q_DLQ, Q_NACK, Q_MSG_TTL, Q_QUEUE_TTL, Q_EXPIRES};
        String[] exchanges = {EX_DLX};

        Connection conn = DemoRunner.newConnection();
        try {
            Channel ch = conn.createChannel();

            ch.exchangeDeclare(EX_DLX, "fanout", true, false, null);
            DemoRunner.declareQueue(ch, Q_DLQ, null);
            ch.queueBind(Q_DLQ, EX_DLX, "");
            DemoRunner.say("dlx", "死信交换机 %s（fanout）+ 死信队列 %s 已就绪", EX_DLX, Q_DLQ);

            // 1) nack(requeue=false)
            DemoRunner.say("dlx", "--- 1) nack(requeue=false) ---");
            DemoRunner.declareQueue(ch, Q_NACK, DemoRunner.args("x-dead-letter-exchange", EX_DLX));
            DemoRunner.publish(ch, Q_NACK, "will-be-rejected");
            final List<Long> tags = Collections.synchronizedList(new ArrayList<>());
            final CountDownLatch one = new CountDownLatch(1);
            String consumerTag = ch.basicConsume(Q_NACK, false, (ct, delivery) -> {
                tags.add(delivery.getEnvelope().getDeliveryTag());
                one.countDown();
            }, ct -> { });
            one.await(3, TimeUnit.SECONDS);
            ch.basicNack(tags.get(0), false, false);
            ch.basicCancel(consumerTag);
            DemoRunner.sleep(800);
            int n1 = drainDlq(ch, "nack(requeue=false) 之后");

            // 2) per-message TTL 到期
            DemoRunner.say("dlx", "--- 2) per-message TTL 到期（官方明确写了的那个） ---");
            DemoRunner.declareQueue(ch, Q_MSG_TTL, DemoRunner.args("x-dead-letter-exchange", EX_DLX));
            DemoRunner.publishWithExpiration(ch, Q_MSG_TTL, "per-message ttl=2000", "2000");
            DemoRunner.sleep(3500);
            int n2 = drainDlq(ch, "per-message TTL 到期之后");

            // 3) 队列级 x-message-ttl 到期（台账 14）
            DemoRunner.say("dlx", "--- 3) 队列级 x-message-ttl 到期（台账 14 的必闭项） ---");
            DemoRunner.declareQueue(ch, Q_QUEUE_TTL, DemoRunner.args(
                    "x-message-ttl", 2000,
                    "x-dead-letter-exchange", EX_DLX));
            DemoRunner.publish(ch, Q_QUEUE_TTL, "queue-level ttl=2000");
            DemoRunner.sleep(3500);
            int n3 = drainDlq(ch, "队列级 x-message-ttl 到期之后");

            // 4) 整队过期（官方写明的例外）
            DemoRunner.say("dlx", "--- 4) x-expires 把整个队列删掉 ---");
            DemoRunner.declareQueue(ch, Q_EXPIRES, DemoRunner.args(
                    "x-expires", 3000,
                    "x-dead-letter-exchange", EX_DLX));
            DemoRunner.publish(ch, Q_EXPIRES, "queue itself expires");
            DemoRunner.sleep(5000);
            boolean gone = DemoRunner.mgmt("/queues/%2F/" + DemoRunner.enc(Q_EXPIRES)).isEmpty();
            DemoRunner.say("dlx", "整队过期后，队列 %s 是否还在：%s", Q_EXPIRES, gone ? "不在了" : "仍在");
            int n4 = drainDlq(ch, "整队过期之后");

            // 结论
            DemoRunner.say("dlx", "=== 四段结论 ===");
            DemoRunner.say("dlx", "1) nack(requeue=false)            → 进死信 %d 条（%s）",
                    n1, n1 > 0 ? "符合文档" : "与文档不符");
            DemoRunner.say("dlx", "2) per-message TTL 到期           → 进死信 %d 条（%s）",
                    n2, n2 > 0 ? "符合文档" : "与文档不符");
            DemoRunner.say("dlx", "3) 队列级 x-message-ttl 到期      → 进死信 %d 条", n3);
            DemoRunner.say("dlx", "4) 整队被 x-expires 删掉          → 进死信 %d 条（%s）",
                    n4, n4 == 0 ? "符合文档的例外说明" : "与文档的例外说明不符");

            DemoRunner.cleanup(ch, queues, exchanges);
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }

    /** 打印死信队列当前深度并读空它；每条打印 body 与 x-death 头。返回读到的条数。 */
    private static int drainDlq(Channel ch, String label) throws IOException {
        int n = DemoRunner.depth(ch, Q_DLQ);
        DemoRunner.say("dlx", "%s → 死信队列深度 = %d", label, n);
        for (int i = 0; i < n; i++) {
            var resp = ch.basicGet(Q_DLQ, true);
            if (resp == null) {
                break;
            }
            DemoRunner.say("dlx", "   #%d body=%s", i + 1, new String(resp.getBody(), StandardCharsets.UTF_8));
            Object xDeath = resp.getProps().getHeaders() == null
                    ? null
                    : resp.getProps().getHeaders().get("x-death");
            DemoRunner.say("dlx", "       x-death=%s", xDeath);
        }
        return n;
    }
}
