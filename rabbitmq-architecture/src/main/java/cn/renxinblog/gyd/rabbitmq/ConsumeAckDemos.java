package cn.renxinblog.gyd.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 第五节与台账 6 的实测：ack 才是「可删除」的条件，prefetch 决定未确认消息的上限。
 *
 * <p>整段围绕管理端的 {@code messages = ready + unacked} 读：投递出去但没 ack 的消息，
 * 在服务端仍然是「未确认」而不是「已读」。
 */
final class ConsumeAckDemos {

    private ConsumeAckDemos() {
    }

    static void run() throws Exception {
        DemoRunner.head("consume-ack");
        DemoRunner.say("reset", "清理上一轮遗留的本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());

        final Connection conn = DemoRunner.newConnection();
        final String q = DemoRunner.P + "ack.q";
        final String q2 = DemoRunner.P + "ack.pull";
        try {
            Channel ch = conn.createChannel();
            // 专职读状态的一条信道：主信道中途会被 close() 掉（那段实验本身要的就是这个），
            // 用同一条件也不会因为「信道关了」而读不到数。被动声明只读，不产生副作用。
            Channel probe = conn.createChannel();
            DemoRunner.declareQueue(ch, q, null);
            for (int i = 1; i <= 5; i++) {
                DemoRunner.publish(ch, q, "msg-" + i);
            }
            DemoRunner.sleep(400);
            DemoRunner.say("ack", "已发布 5 条到 %s", q);
            DemoRunner.state("ack", probe, q);

            DemoRunner.say("ack", "--- 手动 ack，prefetch=2 ---");
            ch.basicQos(2);
            final List<Long> tags = Collections.synchronizedList(new ArrayList<>());
            final AtomicInteger received = new AtomicInteger();
            final CountDownLatch firstTwo = new CountDownLatch(2);
            DeliverCallback onDelivery = (consumerTag, delivery) -> {
                tags.add(delivery.getEnvelope().getDeliveryTag());
                received.incrementAndGet();
                DemoRunner.say("ack", "  投递到客户端：deliveryTag=%d body=%s（尚未 ack）",
                        delivery.getEnvelope().getDeliveryTag(),
                        new String(delivery.getBody(), StandardCharsets.UTF_8));
                firstTwo.countDown();
            };
            ch.basicConsume(q, false, onDelivery, ct -> { });

            firstTwo.await(5, TimeUnit.SECONDS);
            DemoRunner.sleep(1500);
            DemoRunner.say("ack", "prefetch=2，收到第 2 条后再等 1.5 秒：共投递 %d 条", received.get());
            DemoRunner.say("ack", "→ 未确认消息已达上限，第 3 条不会提前送过来");
            DemoRunner.state("ack", probe, q);

            DemoRunner.say("ack", "--- ack 第 1 条 ---");
            ch.basicAck(tags.get(0), false);
            DemoRunner.sleep(1200);
            DemoRunner.say("ack", "ack 之后共投递 %d 条（上限腾出一个名额，第 3 条这才来）", received.get());
            DemoRunner.state("ack", probe, q);

            DemoRunner.say("ack", "--- 直接关掉信道，剩下的不 ack ---");
            DemoRunner.say("ack", "此刻已投递 %d 条、其中已 ack 1 条，剩下 %d 条未确认；不发 basicCancel，直接 close",
                    tags.size(), tags.size() - 1);
            ch.close();
            DemoRunner.sleep(1000);
            DemoRunner.state("ack", probe, q);
            int afterClose = DemoRunner.depth(probe, q);
            DemoRunner.say("ack", "→ 信道一关，未确认消息自动退回 ready：AMQP 实时 ready=%d，%s",
                    afterClose, afterClose == 4
                            ? "5 条里 1 条已 ack 被永久删除、剩下 4 条一条没丢"
                            : "与「5-1=4」不符，按实测记录");

            DemoRunner.say("ack", "--- 另一种收手势：basicCancel（连接与信道都留着） ---");
            Channel ch1 = conn.createChannel();
            ch1.basicQos(2);
            final AtomicInteger received2 = new AtomicInteger();
            String tag2 = ch1.basicConsume(q, false, (ct, d) -> received2.incrementAndGet(), ct -> { });
            DemoRunner.sleep(1500);
            DemoRunner.say("ack", "新消费者又取走 %d 条（都没 ack）", received2.get());
            DemoRunner.state("ack", probe, q);

            ch1.basicCancel(tag2);
            DemoRunner.sleep(2000);
            DemoRunner.state("ack", probe, q);
            int afterCancel = DemoRunner.depth(probe, q);
            DemoRunner.say("ack", "取消消费者之后 ready=%d → %s", afterCancel, afterCancel == 4
                    ? "取消消费者本身就足以让未确认消息退回 ready"
                    : "取消消费者并没有让它们退回 ready——退不退是看信道/连接关没关（下面接着验这一步）");

            ch1.close();
            DemoRunner.sleep(1200);
            DemoRunner.state("ack", probe, q);
            int afterChClose = DemoRunner.depth(probe, q);
            DemoRunner.say("ack", "再关掉这条信道之后 ready=%d → %s", afterChClose, afterChClose == 4
                    ? "回到 4：5 条里 1 条已 ack 被永久删除，剩下 4 条一条没丢"
                    : "没回到 4，按实测记录");

            DemoRunner.say("ack", "--- push 之外的另一种取法：basicGet（pull） ---");
            Channel ch2 = conn.createChannel();
            DemoRunner.declareQueue(ch2, q2, null);
            DemoRunner.publish(ch2, q2, "pull-1");
            DemoRunner.publish(ch2, q2, "pull-2");
            DemoRunner.sleep(400);
            for (int i = 1; i <= 3; i++) {
                String body = DemoRunner.getOnce(ch2, q2, true);
                DemoRunner.say("ack", "第 %d 次 basicGet → %s", i, body == null ? "null（队列已空）" : body);
            }

            DemoRunner.cleanup(ch2, new String[]{q, q2}, new String[]{});
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }
}
