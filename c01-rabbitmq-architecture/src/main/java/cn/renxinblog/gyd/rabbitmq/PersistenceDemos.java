package cn.renxinblog.gyd.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 第四节的实测：持久化与复制是两个独立维度。
 *
 * <p>要观察「重启后哪条还在」，就必须跨过 broker 的一次重启，而一个进程里的 demo
 * 自己重启不了容器。所以这一段拆成两条命令，中间由人来重启：
 *
 * <pre>
 *   1) ./gradlew :c01-rabbitmq-architecture:run --args="persistence-write"
 *   2) docker restart gyd-rabbitmq &amp;&amp; 等健康
 *   3) ./gradlew :c01-rabbitmq-architecture:run --args="persistence-read"
 * </pre>
 *
 * <p>被观察的对照只有一组：**同一个 durable 队列**里放两条 deliveryMode=2（持久化）
 * 和一条 deliveryMode=1（非持久化）的消息。队列是持久的、消息级别不同——重启之后
 * 少了哪一条，就说明「队列持久化」与「消息持久化」不是一回事。
 *
 * <p>刻意**不做**「非持久化队列」那一组：4.3 起「非持久化且非排他」的队列默认被拒
 * （{@code transient_nonexcl_queues}，见第七节），剩下的非持久化队列只能是排他的，
 * 而排他队列本来就随连接关闭而销毁——它活不到重启那一刻，测不出「重启时被删除」。
 *
 * <p>还有一处必须留在这里的坑：{@code durable=true} 的队列**重声明**不会丢消息，
 * 所以 read 阶段必须用 {@link DemoRunner#declareQueue} 声明一次再读（幂等），
 * 而不是先删后建。
 */
final class PersistenceDemos {

    private static final String Q = DemoRunner.P + "persist.q";
    private static final String[] QUEUES = {Q};
    private static final String[] EXCHANGES = {};

    private PersistenceDemos() {
    }

    /** 第一阶段：把「队列持久化 + 消息级别混合」的一组消息放进队列。 */
    static void write() throws Exception {
        DemoRunner.head("persistence-write");
        DemoRunner.say("persist", "清理上一轮遗留的本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());

        Connection conn = DemoRunner.newConnection();
        try {
            Channel ch = conn.createChannel();
            DemoRunner.declareQueue(ch, Q, null);
            DemoRunner.say("persist", "已声明（durable=true / exclusive=false / autoDelete=false）：%s", Q);

            ch.basicPublish("", Q, persistent(), DemoRunner.utf8("persistent-1"));
            ch.basicPublish("", Q, persistent(), DemoRunner.utf8("persistent-2"));
            ch.basicPublish("", Q, nonPersistent(), DemoRunner.utf8("transient-1"));
            DemoRunner.say("persist", "已发布 3 条：persistent-1 / persistent-2（deliveryMode=2）"
                    + " 与 transient-1（deliveryMode=1）");

            DemoRunner.sleep(600);
            DemoRunner.state("persist", ch, Q);
            DemoRunner.say("persist", "现在请重启 broker，再跑 persistence-read：");
            DemoRunner.say("persist", "  docker restart gyd-rabbitmq");
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }

    /** 第二阶段：重启之后看还剩几条、哪几条。 */
    static void read() throws Exception {
        DemoRunner.head("persistence-read");

        Connection conn = DemoRunner.newConnection();
        try {
            Channel ch = conn.createChannel();
            // 幂等重声明：durable 队列重新声明不会清空消息，这一步只是为了确保队列存在
            DemoRunner.declareQueue(ch, Q, null);
            DemoRunner.say("persist", "队列 %s 在重启后仍然存在（durable=true）", Q);
            DemoRunner.state("persist", ch, Q);

            List<String> left = DemoRunner.drain(ch, Q, true, null, 3, 2000);
            DemoRunner.say("persist", "重启后读到的消息：%s", left);
            DemoRunner.say("persist", "→ %s", report(left));

            DemoRunner.cleanup(ch, QUEUES, EXCHANGES);
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }

    private static String report(List<String> left) {
        boolean p1 = left.contains("persistent-1");
        boolean p2 = left.contains("persistent-2");
        boolean t1 = left.contains("transient-1");
        if (p1 && p2 && !t1) {
            return "两条 deliveryMode=2 的都还在、deliveryMode=1 的那条没了："
                    + "**队列持久化管的是队列本身，消息能不能熬过重启看的是消息自己的 deliveryMode**";
        }
        if (p1 && p2 && t1) {
            return "三条都还在——与「transient 消息恢复时被丢弃」的文档表述不符，按实测记录";
        }
        if (!p1 && !p2) {
            return "两条持久化消息也没了——**这不是 deliveryMode 的问题**："
                    + "先去确认重启前队列是 durable、且消息确实是按 deliveryMode=2 发的（按实测记录）";
        }
        return "结果既不是「全在」也不是「只少 transient 那条」，按实测记录：" + left;
    }

    /** deliveryMode=2：消息标记为持久化。 */
    private static AMQP.BasicProperties persistent() {
        return new AMQP.BasicProperties.Builder().deliveryMode(2).build();
    }

    /** deliveryMode=1：消息不标记持久化（方法名不能叫 transient，那是关键字）。 */
    private static AMQP.BasicProperties nonPersistent() {
        return new AMQP.BasicProperties.Builder().deliveryMode(1).build();
    }
}
