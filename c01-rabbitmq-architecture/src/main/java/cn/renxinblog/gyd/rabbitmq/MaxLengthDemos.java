package cn.renxinblog.gyd.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 第六节长度限制部分的实测：{@code x-max-length} 到了之后，超出的消息去哪儿，
 * 由 {@code x-overflow} 决定。三档的差别就在「发送端能不能感知」。
 */
final class MaxLengthDemos {

    private static final String Q_DROP_HEAD = DemoRunner.P + "maxlen.drophead";
    private static final String Q_REJECT = DemoRunner.P + "maxlen.reject";
    private static final String Q_REJECT_DLX = DemoRunner.P + "maxlen.rejectdlx";
    private static final String Q_QUORUM_DLX = DemoRunner.P + "maxlen.quorum.rjdlx";
    private static final String EX_DLX = DemoRunner.P + "maxlen.dlx";
    private static final String Q_DLQ = DemoRunner.P + "maxlen.dlq";

    private MaxLengthDemos() {
    }

    static void run() throws Exception {
        DemoRunner.head("max-length");
        DemoRunner.say("reset", "清理上一轮遗留的本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());

        String[] queues = {Q_DROP_HEAD, Q_REJECT, Q_REJECT_DLX, Q_QUORUM_DLX, Q_DLQ};
        String[] exchanges = {EX_DLX};

        Connection conn = DemoRunner.newConnection();
        try {
            Channel ch = conn.createChannel();

            // 1) drop-head（默认）
            DemoRunner.say("max-length", "--- 1) x-max-length=3，x-overflow 缺省（drop-head） ---");
            DemoRunner.declareQueue(ch, Q_DROP_HEAD, DemoRunner.args("x-max-length", 3));
            for (int i = 1; i <= 5; i++) {
                DemoRunner.publish(ch, Q_DROP_HEAD, "drop-" + i);
            }
            DemoRunner.sleep(600);
            DemoRunner.say("max-length", "依次发了 drop-1..drop-5，队列深度 = %d", DemoRunner.depth(ch, Q_DROP_HEAD));
            for (int i = 1; i <= 3; i++) {
                DemoRunner.say("max-length", "  取出的第 %d 条 = %s", i, DemoRunner.getOnce(ch, Q_DROP_HEAD, true));
            }
            DemoRunner.say("max-length", "→ 留下的是最后 3 条：超出的从队头（最早的）开始丢");

            // 2) reject-publish
            DemoRunner.say("max-length", "--- 2) x-max-length=3，x-overflow=reject-publish ---");
            DemoRunner.declareQueue(ch, Q_REJECT, DemoRunner.args(
                    "x-max-length", 3,
                    "x-overflow", "reject-publish"));
            ch.confirmSelect();
            final AtomicInteger acks = new AtomicInteger();
            final AtomicInteger nacks = new AtomicInteger();
            ch.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long deliveryTag, boolean multiple) {
                    acks.incrementAndGet();
                }

                @Override
                public void handleNack(long deliveryTag, boolean multiple) {
                    nacks.incrementAndGet();
                }
            });
            for (int i = 1; i <= 5; i++) {
                DemoRunner.publish(ch, Q_REJECT, "reject-" + i);
            }
            boolean allConfirmed;
            try {
                allConfirmed = ch.waitForConfirms(10_000);
            } catch (Exception e) {
                allConfirmed = false;
                DemoRunner.say("max-length", "waitForConfirms 抛出：%s", e);
            }
            DemoRunner.sleep(400);
            DemoRunner.say("max-length", "发了 5 条：waitForConfirms=%s；确认回调 ack=%d 次 nack=%d 次",
                    allConfirmed, acks.get(), nacks.get());
            DemoRunner.say("max-length", "队列深度 = %d", DemoRunner.depth(ch, Q_REJECT));
            DemoRunner.say("max-length", "→ 发送端能感知到被拒（这一档与前两档的分界就在这里）");

            // 3) reject-publish-dlx
            DemoRunner.say("max-length", "--- 3) x-max-length=3，x-overflow=reject-publish-dlx ---");
            ch.exchangeDeclare(EX_DLX, "fanout", true, false, null);
            DemoRunner.declareQueue(ch, Q_DLQ, null);
            ch.queueBind(Q_DLQ, EX_DLX, "");
            DemoRunner.declareQueue(ch, Q_REJECT_DLX, DemoRunner.args(
                    "x-max-length", 3,
                    "x-overflow", "reject-publish-dlx",
                    "x-dead-letter-exchange", EX_DLX));
            for (int i = 1; i <= 5; i++) {
                DemoRunner.publish(ch, Q_REJECT_DLX, "rjd-" + i);
            }
            DemoRunner.sleep(800);
            DemoRunner.say("max-length", "发了 5 条：主队列深度 = %d，死信队列深度 = %d",
                    DemoRunner.depth(ch, Q_REJECT_DLX), DemoRunner.depth(ch, Q_DLQ));
            int dlq = DemoRunner.depth(ch, Q_DLQ);
            for (int i = 0; i < dlq; i++) {
                var resp = ch.basicGet(Q_DLQ, true);
                if (resp == null) {
                    break;
                }
                Object xDeath = resp.getProps().getHeaders() == null
                        ? null
                        : resp.getProps().getHeaders().get("x-death");
                DemoRunner.say("max-length", "   死信 #%d body=%s x-death=%s",
                        i + 1, new String(resp.getBody(), StandardCharsets.UTF_8), xDeath);
            }
            DemoRunner.say("max-length", "→ 被拒的那几条改道进了死信队列");

            // 4) quorum 上是否支持 reject-publish-dlx（台账 15 的「quorum 例外」）
            //
            // 官方 classic ↔ quorum 特性矩阵把「队列长度限制」标成
            // 「yes（除 x-overflow: reject-publish-dlx）」，等于暗示 quorum 不支持这一档。
            // 但「声明被接受」不等于「行为生效」，所以不能只看声明成没成，
            // 必须把 5 条打进去，看溢出那两条到底去了哪儿。
            DemoRunner.say("max-length", "--- 4) 在 quorum 队列上用 reject-publish-dlx ---");
            boolean quorumDeclared = false;
            try {
                DemoRunner.declareQueue(ch, Q_QUORUM_DLX, DemoRunner.args(
                        "x-queue-type", "quorum",
                        "x-max-length", 3,
                        "x-overflow", "reject-publish-dlx",
                        "x-dead-letter-exchange", EX_DLX));
                quorumDeclared = true;
                DemoRunner.say("max-length", "quorum 队列接受了这组声明参数；下面验行为，而不只是验声明");
            } catch (Exception e) {
                DemoRunner.reportError("max-length", e, conn, ch);
                ch = DemoRunner.reopen(conn, ch);
            }
            if (quorumDeclared) {
                for (int i = 1; i <= 5; i++) {
                    DemoRunner.publish(ch, Q_QUORUM_DLX, "qq-" + i);
                }
                DemoRunner.sleep(1000);
                int mainDepth = DemoRunner.depth(ch, Q_QUORUM_DLX);
                int dlqDepth = DemoRunner.depth(ch, Q_DLQ);
                DemoRunner.say("max-length", "发了 5 条：quorum 主队列深度 = %d，死信队列深度 = %d",
                        mainDepth, dlqDepth);

                // 只看深度是不够的：官方矩阵与正文都写明 quorum「不支持 reject-publish-dlx」，
                // 而不支持时最可能的退路是 drop-head——但 drop-head 丢掉的旧消息**同样会进死信**
                // （长度超限是死信的第三种触发）。所以两种假设下「主 3 + 死信 2」长得一模一样，
                // 必须把死信的消息体读出来才能分开：
                //   读到 qq-4 / qq-5 → 被拒的是「最新发布的两条」→ reject-publish-dlx 真的生效了
                //   读到 qq-1 / qq-2 → 丢的是「最早的两条」→ 实际退回了 drop-head
                java.util.List<String> bodies = new java.util.ArrayList<>();
                for (int i = 0; i < dlqDepth; i++) {
                    var resp = ch.basicGet(Q_DLQ, true);
                    if (resp == null) {
                        break;
                    }
                    String body = new String(resp.getBody(), StandardCharsets.UTF_8);
                    bodies.add(body);
                    Object xDeath = resp.getProps().getHeaders() == null
                            ? null
                            : resp.getProps().getHeaders().get("x-death");
                    DemoRunner.say("max-length", "   quorum 死信 #%d body=%s x-death=%s", i + 1, body, xDeath);
                }
                boolean rejectedNewest = bodies.equals(java.util.List.of("qq-4", "qq-5"));
                boolean droppedOldest = bodies.equals(java.util.List.of("qq-1", "qq-2"));
                DemoRunner.say("max-length", "→ %s", rejectedNewest
                        ? "被死信的是最新发布的两条（qq-4/qq-5）：reject-publish-dlx 在 4.3.5 上确实生效了，"
                        + "与官方「quorum 不支持 reject-publish-dlx」的措辞不符——正文按实测写，并标注文档措辞"
                        : droppedOldest
                        ? "被死信的是最早的两条（qq-1/qq-2）：实际退回了 drop-head。"
                        + "声明被接受但这一档没生效，与官方「quorum 不支持 reject-publish-dlx」一致"
                        : "既不是最新两条也不是最早两条，按实测记录：" + bodies);
            }

            DemoRunner.cleanup(ch, queues, exchanges);
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }
}
