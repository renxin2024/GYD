package cn.renxinblog.gyd.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.util.List;

/**
 * 第三节的实测：三种队列类型的分野是<b>消费语义</b>，不是性能档位。
 *
 * <ul>
 *   <li>classic / quorum 的声明，以及管理端看到的 {@code type}</li>
 *   <li>已存在的队列换 {@code x-queue-type} → 被拒</li>
 *   <li>stream 上两次读同一批消息 → 非破坏性读</li>
 *   <li>classic 上同样的两次读 → 第一次读完就没了</li>
 * </ul>
 */
final class QueueTypeDemos {

    private static final String Q_CLASSIC = DemoRunner.P + "type.classic";
    private static final String Q_QUORUM = DemoRunner.P + "type.quorum";
    private static final String Q_STREAM = DemoRunner.P + "type.stream";

    private static final String[] QUEUES = {Q_CLASSIC, Q_QUORUM, Q_STREAM};

    private QueueTypeDemos() {
    }

    static void run() throws Exception {
        DemoRunner.head("queue-types");
        DemoRunner.say("reset", "清理上一轮遗留的本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());

        Connection conn = DemoRunner.newConnection();
        try {
            Channel ch = conn.createChannel();

            DemoRunner.say("type", "--- 1) 不写 x-queue-type，拿到的是哪种 ---");
            DemoRunner.declareQueue(ch, Q_CLASSIC, null);
            DemoRunner.say("type", "已声明 %s（不带任何额外参数）", Q_CLASSIC);
            DemoRunner.state("type", ch, Q_CLASSIC);

            DemoRunner.say("type", "--- 2) 显式声明 quorum ---");
            DemoRunner.declareQueue(ch, Q_QUORUM, DemoRunner.args("x-queue-type", "quorum"));
            DemoRunner.say("type", "已声明 %s（x-queue-type=quorum）", Q_QUORUM);
            DemoRunner.state("type", ch, Q_QUORUM);

            DemoRunner.say("type", "--- 3) 把已存在的 classic 队列重新声明成 quorum ---");
            DemoRunner.say("type", "对 %s 重新声明 x-queue-type=quorum", Q_CLASSIC);
            try {
                DemoRunner.declareQueue(ch, Q_CLASSIC, DemoRunner.args("x-queue-type", "quorum"));
                DemoRunner.say("type", "意外：声明成功了");
            } catch (Exception e) {
                DemoRunner.reportError("type", e, conn, ch);
            }
            ch = DemoRunner.reopen(conn, ch);
            DemoRunner.say("type", "被拒之后，原队列有没有被改掉？");
            DemoRunner.state("type", ch, Q_CLASSIC);
            DemoRunner.say("type", "→ 类型在声明期就锁定了；改它等于「拿一组不同的参数去比同一份声明」，不会生效");

            DemoRunner.say("type", "--- 4) stream：同一批消息能不能被读第二次 ---");
            // stream 的消费者必须先显式设过 prefetch，否则 broker 会直接以
            // 406 PRECONDITION_FAILED - consumer prefetch count is not set for stream queue
            // 关掉信道（实测撞到过）。官方 stream 示例里这一句是必备的，不是可选优化。
            ch.basicQos(10);
            DemoRunner.declareQueue(ch, Q_STREAM, DemoRunner.args("x-queue-type", "stream"));
            for (int i = 0; i < 5; i++) {
                DemoRunner.publish(ch, Q_STREAM, "stream-" + i);
            }
            DemoRunner.sleep(500);
            List<String> first = DemoRunner.drain(ch, Q_STREAM, false,
                    DemoRunner.args("x-stream-offset", "first"), 5, 5000);
            DemoRunner.say("type", "第 1 个消费者从 x-stream-offset=first 读到 %d 条：%s",
                    first.size(), first);
            List<String> second = DemoRunner.drain(ch, Q_STREAM, false,
                    DemoRunner.args("x-stream-offset", "first"), 5, 5000);
            DemoRunner.say("type", "第 2 个消费者同样从 first 读，读到 %d 条：%s",
                    second.size(), second);
            DemoRunner.state("type", ch, Q_STREAM);
            DemoRunner.say("type", "→ 两次读到同一批：stream 是非破坏性读，读不等于删");

            DemoRunner.say("type", "--- 5) 对照：classic 上同样的两次读 ---");
            for (int i = 0; i < 5; i++) {
                DemoRunner.publish(ch, Q_CLASSIC, "classic-" + i);
            }
            DemoRunner.sleep(500);
            List<String> c1 = DemoRunner.drain(ch, Q_CLASSIC, false, null, 5, 5000);
            DemoRunner.say("type", "classic 第 1 个消费者读到 %d 条", c1.size());
            List<String> c2 = DemoRunner.drain(ch, Q_CLASSIC, false, null, 1, 1500);
            DemoRunner.say("type", "classic 第 2 个消费者读到 %d 条（期望 0）", c2.size());
            DemoRunner.state("type", ch, Q_CLASSIC);
            DemoRunner.say("type", "→ 消费即删除：同一批消息在 classic 上读不到第二次");

            DemoRunner.cleanup(ch, QUEUES, new String[]{});
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }
}
