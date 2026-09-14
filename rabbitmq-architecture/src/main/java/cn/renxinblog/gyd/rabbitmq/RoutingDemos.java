package cn.renxinblog.gyd.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;

/**
 * 第二节的实测：一条消息的落点由「交换机类型 + 绑定」共同决定，落点可以是 0。
 *
 * <ul>
 *   <li>{@code route} —— 同一个路由键在 direct / fanout / topic 下的落点对比，含「无绑定 → 落 0 个队列」</li>
 *   <li>{@code default-exchange} —— 空串交换机名到底走了什么，以及能不能对它 bind</li>
 * </ul>
 */
final class RoutingDemos {

    private static final String EX_DIRECT = DemoRunner.P + "ex.direct";
    private static final String EX_FANOUT = DemoRunner.P + "ex.fanout";
    private static final String EX_TOPIC = DemoRunner.P + "ex.topic";

    private static final String Q_DIRECT_A = DemoRunner.P + "route.direct.a";
    private static final String Q_DIRECT_B = DemoRunner.P + "route.direct.b";
    private static final String Q_FANOUT_A = DemoRunner.P + "route.fanout.a";
    private static final String Q_FANOUT_B = DemoRunner.P + "route.fanout.b";
    private static final String Q_TOPIC_A = DemoRunner.P + "route.topic.a";
    private static final String Q_TOPIC_B = DemoRunner.P + "route.topic.b";

    private static final String[] QUEUES = {
            Q_DIRECT_A, Q_DIRECT_B, Q_FANOUT_A, Q_FANOUT_B, Q_TOPIC_A, Q_TOPIC_B
    };
    private static final String[] EXCHANGES = {EX_DIRECT, EX_FANOUT, EX_TOPIC};

    private RoutingDemos() {
    }

    // ---------------------------------------------------------------- route

    static void route() throws Exception {
        DemoRunner.head("route");
        DemoRunner.say("reset", "清理上一轮遗留的本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());

        Connection conn = DemoRunner.newConnection();
        try {
            Channel ch = conn.createChannel();

            ch.exchangeDeclare(EX_DIRECT, "direct", true, false, null);
            ch.exchangeDeclare(EX_FANOUT, "fanout", true, false, null);
            ch.exchangeDeclare(EX_TOPIC, "topic", true, false, null);
            DemoRunner.say("route", "声明三种交换机：direct=%s fanout=%s topic=%s",
                    EX_DIRECT, EX_FANOUT, EX_TOPIC);

            for (String q : QUEUES) {
                DemoRunner.declareQueue(ch, q, null);
            }

            // direct：路由键必须逐字相同
            ch.queueBind(Q_DIRECT_A, EX_DIRECT, "order.created");
            ch.queueBind(Q_DIRECT_B, EX_DIRECT, "order.paid");
            // fanout：忽略路由键，投给它认识的每一个队列
            ch.queueBind(Q_FANOUT_A, EX_FANOUT, "");
            ch.queueBind(Q_FANOUT_B, EX_FANOUT, "");
            // topic：路由键按「词」匹配，* 恰好一个词
            ch.queueBind(Q_TOPIC_A, EX_TOPIC, "order.*");
            ch.queueBind(Q_TOPIC_B, EX_TOPIC, "*.created");
            DemoRunner.say("route", "绑定：direct 只认 order.created / order.paid；"
                    + "fanout 不看键；topic 认 order.* 与 *.created");

            round(ch, "order.created");
            round(ch, "no.match.at.all");

            DemoRunner.say("route", "→ 结论：同一句 basicPublish、同一个路由键，"
                    + "落点数量完全由「交换机类型 + 绑定」决定，与队列类型无关");

            DemoRunner.cleanup(ch, QUEUES, EXCHANGES);
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }

    private static void round(Channel ch, String routingKey) throws IOException {
        DemoRunner.say("route", "--- 路由键 \"%s\" ---", routingKey);
        DemoRunner.say("route", "发布前深度  %s", depths(ch));

        boolean threw = false;
        String failure = null;
        try {
            for (String ex : EXCHANGES) {
                DemoRunner.publish(ch, ex, routingKey, "rk=" + routingKey);
            }
        } catch (Exception e) {
            threw = true;
            failure = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        // 投递是异步落账的，给 broker 一点时间再读深度
        DemoRunner.sleep(400);

        DemoRunner.say("route", "发布后深度  %s", depths(ch));
        DemoRunner.say("route", "发送端抛异常 = %s%s", threw, failure == null ? "" : "（" + failure + "）");
    }

    private static String depths(Channel ch) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (String q : QUEUES) {
            sb.append(q, DemoRunner.P.length(), q.length())
                    .append('=')
                    .append(DemoRunner.depth(ch, q))
                    .append("  ");
        }
        return sb.toString().trim();
    }

    // ---------------------------------------------------------------- default exchange

    static void defaultExchange() throws Exception {
        DemoRunner.head("default-exchange");
        DemoRunner.say("reset", "清理上一轮遗留的本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());

        String q = DemoRunner.P + "default.q";
        String[] queues = {q};
        String[] exchanges = {};

        Connection conn = DemoRunner.newConnection();
        try {
            Channel ch = conn.createChannel();
            DemoRunner.declareQueue(ch, q, null);
            DemoRunner.say("default-exchange", "队列 %s 已声明", q);

            DemoRunner.say("default-exchange", "--- 1) 交换机名用空串发布，路由键写队列名 ---");
            DemoRunner.publish(ch, "", q, "sent to the default exchange");
            DemoRunner.sleep(400);
            DemoRunner.say("default-exchange", "队列深度 = %d（期望 1）", DemoRunner.depth(ch, q));

            DemoRunner.say("default-exchange", "--- 2) 每个队列都会有一条以自己名字为路由键的自动绑定 ---");
            DemoRunner.say("default-exchange", "（下面这行取自管理端，不是客户端猜的）");
            DemoRunner.mgmtBindings("default-exchange", q);

            DemoRunner.say("default-exchange", "--- 3) 这个内建交换机不许被操作 ---");
            ch = probeDefault(conn, ch, "被动声明 exchangeDeclarePassive(\"\")",
                    c -> c.exchangeDeclarePassive(""));

            DemoRunner.say("default-exchange", "--- 4) 也不许往它上面加绑定 ---");
            ch = probeDefault(conn, ch, "对它执行 queueBind",
                    c -> c.queueBind(q, "", "some.key"));

            DemoRunner.say("default-exchange", "→ 空串名的 default exchange 是内建实体：每个队列按自己的名字"
                    + "自动和它绑好，所以「发到队列名」其实一直在走交换机，只是名字被省略了；"
                    + "而它本身既不能被被动声明、也不能新增绑定——两次都是 403");
            DemoRunner.cleanup(ch, queues, exchanges);
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }

    private interface ChannelAction {
        void run(Channel ch) throws Exception;
    }

    /**
     * 在内建 default exchange 上做一次「预期会被拒」的操作。
     *
     * <p>被拒会把信道关掉（403 是信道级错误），所以每次都要换一条信道回来，
     * 顺便也就演示了「关掉的只是信道」。
     */
    private static Channel probeDefault(Connection conn, Channel ch, String what, ChannelAction action)
            throws Exception {
        try {
            action.run(ch);
            DemoRunner.say("default-exchange", "%s → 竟然成功了", what);
        } catch (Exception e) {
            DemoRunner.reportError("default-exchange", e, conn, ch);
        }
        Channel next = DemoRunner.reopen(conn, ch);
        DemoRunner.say("default-exchange", "换一条 channel #%d → 可用，connection 仍活着",
                next.getChannelNumber());
        return next;
    }
}
