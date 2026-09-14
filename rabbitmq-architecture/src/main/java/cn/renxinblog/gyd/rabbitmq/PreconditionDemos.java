package cn.renxinblog.gyd.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.util.ArrayList;
import java.util.List;

/**
 * 第七节的实测：声明为什么会被拒，以及被拒时 broker 关掉的是 channel 还是 connection。
 *
 * <p>五个用例的「手势」都是「对一个已存在的、或名字不规范的队列做声明」，但落到的错误类
 * 不一样。最后回一张表，方便正文直接引用。
 */
final class PreconditionDemos {

    private PreconditionDemos() {
    }

    static void run() throws Exception {
        DemoRunner.head("precondition");
        DemoRunner.say("reset", "清理上一轮遗留的本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());

        List<String> recap = new ArrayList<>();
        String[] queues = {
                DemoRunner.P + "pre.arg",
                DemoRunner.P + "pre.type",
                DemoRunner.P + "pre.transient"
        };

        Connection conn = DemoRunner.newConnection();
        try {
            Channel ch = conn.createChannel();

            // 1) 参数不一致：给一个已存在的队列补一个 TTL 参数重新声明
            DemoRunner.say("pre", "--- 1) 同名队列，加一个 x-message-ttl 再声明一次 ---");
            String q1 = queues[0];
            DemoRunner.declareQueue(ch, q1, null);
            DemoRunner.say("pre", "%s 已存在（无任何额外参数），现在带 x-message-ttl=1000 再声明一次", q1);
            try {
                DemoRunner.declareQueue(ch, q1, DemoRunner.args("x-message-ttl", 1000));
                DemoRunner.say("pre", "意外：声明成功");
                recap.add("参数不一致 → 竟然成功");
            } catch (Exception e) {
                DemoRunner.reportError("pre", e, conn, ch);
                recap.add("参数不一致（补 TTL） → " + DemoRunner.classify(e));
            }
            conn = ensureOpen(conn);
            ch = DemoRunner.reopen(conn, ch);

            // 2) 类型不一致
            DemoRunner.say("pre", "--- 2) 同名队列，换 x-queue-type 再声明一次 ---");
            String q2 = queues[1];
            DemoRunner.declareQueue(ch, q2, null);
            DemoRunner.say("pre", "%s 已存在（classic），现在按 quorum 再声明一次", q2);
            try {
                DemoRunner.declareQueue(ch, q2, DemoRunner.args("x-queue-type", "quorum"));
                DemoRunner.say("pre", "意外：声明成功");
                recap.add("类型不一致 → 竟然成功");
            } catch (Exception e) {
                DemoRunner.reportError("pre", e, conn, ch);
                recap.add("类型不一致（classic→quorum） → " + DemoRunner.classify(e));
            }
            conn = ensureOpen(conn);
            ch = DemoRunner.reopen(conn, ch);

            // 3) 名字本来就不该被接受的两类
            DemoRunner.say("pre", "--- 3a) 队列名里带空格 ---");
            String badName = DemoRunner.P + "pre.bad name";
            DemoRunner.say("pre", "声明队列名 \"%s\"", badName);
            try {
                DemoRunner.declareQueue(ch, badName, null);
                DemoRunner.say("pre", "声明成功 → 空格本身不是非法字符，这条名字被放行");
                recap.add("队列名带空格 → 放行（不是错误）");
            } catch (Exception e) {
                DemoRunner.reportError("pre", e, conn, ch);
                recap.add("队列名带空格 → " + DemoRunner.classify(e));
            }
            conn = ensureOpen(conn);
            ch = DemoRunner.reopen(conn, ch);

            DemoRunner.say("pre", "--- 3b) 队列名用保留前缀 amq. ---");
            String reserved = "amq." + DemoRunner.P + "pre.reserved";
            DemoRunner.say("pre", "声明队列名 \"%s\"", reserved);
            try {
                DemoRunner.declareQueue(ch, reserved, null);
                DemoRunner.say("pre", "声明成功 → 本部署没有拦这个前缀");
                recap.add("保留前缀 amq. → 放行");
            } catch (Exception e) {
                DemoRunner.reportError("pre", e, conn, ch);
                recap.add("保留前缀 amq. → " + DemoRunner.classify(e));
            }
            conn = ensureOpen(conn);
            ch = DemoRunner.reopen(conn, ch);

            // 4) 本部署默认拒绝的那条弃用组合
            DemoRunner.say("pre", "--- 4) 声明一个「非持久化且非排他」的队列 ---");
            String q4 = queues[2];
            DemoRunner.say("pre", "durable=false + exclusive=false 在 4.3 已被列为默认拒绝的弃用组合");
            try {
                ch.queueDeclare(q4, false, false, false, null);
                DemoRunner.say("pre", "声明成功（说明本部署放行了这条组合）");
                recap.add("transient 非排他队列 → 放行");
            } catch (Exception e) {
                DemoRunner.reportError("pre", e, conn, ch);
                recap.add("transient 非排他队列 → " + DemoRunner.classify(e));
            }
            conn = ensureOpen(conn);
            ch = DemoRunner.reopen(conn, ch);

            // 5) 独占队列被另一条连接访问
            DemoRunner.say("pre", "--- 5) 用另一条 connection 访问独占队列 ---");
            String q5 = DemoRunner.P + "pre.exclusive";
            Channel chA = conn.createChannel();
            chA.queueDeclare(q5, false, true, true, null);
            DemoRunner.say("pre", "在连接 A 上声明了独占队列 %s", q5);

            Connection connB = DemoRunner.newConnection();
            try {
                Channel chB = connB.createChannel();
                DemoRunner.say("pre", "用连接 B 被动声明同一个队列");
                try {
                    chB.queueDeclarePassive(q5);
                    DemoRunner.say("pre", "意外：访问成功");
                    recap.add("跨连接访问独占队列 → 竟然成功");
                } catch (Exception e) {
                    DemoRunner.reportError("pre", e, connB, chB);
                    recap.add("跨连接访问独占队列 → " + DemoRunner.classify(e));
                }
            } finally {
                DemoRunner.closeQuietly(connB);
            }
            chA.queueDelete(q5);

            DemoRunner.say("pre", "=== 归类回表 ===");
            for (String line : recap) {
                DemoRunner.say("pre", "  " + line);
            }

            DemoRunner.cleanup(ch, queues, new String[]{});
            DemoRunner.say("pre", "补一次前缀清理（3a 那种没登记进清单的名字也能收掉）：%d 项",
                    DemoRunner.mgmtPurgePrefix());
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }

    /**
     * 连接级错误会把整条连接关掉，下一段要用的连接必须重建。
     *
     * <p>实测：案例 4 报的是 {@code 541 INTERNAL_ERROR}——这是<b>连接级</b>错误，
     * 它会连 connection 一起关掉，所以「换一条信道继续」那套在它身上不成立，
     * 必须新开一条连接。信道级错误则不需要这一步。
     */
    private static Connection ensureOpen(Connection conn) throws Exception {
        if (conn != null && conn.isOpen()) {
            return conn;
        }
        DemoRunner.say("pre", "→ 上一条连接已被关掉（连接级错误的代价）；换一条全新连接继续");
        return DemoRunner.newConnection();
    }
}
