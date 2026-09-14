package cn.renxinblog.gyd.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.Frame;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 第一节的实测：vhost / connection / channel 三层作用域，以及「被拒时关掉的是哪一层」。
 *
 * <ul>
 *   <li>A —— 一个 connection 上开多条 channel；队列挂在 vhost 上，不随声明它的 channel 走</li>
 *   <li>A2 —— 信道级协议错误：broker 关掉 channel，connection 仍可用</li>
 *   <li>B —— 一组「同一类手势、不同落点」的探针，逐条看错误码落在哪一级</li>
 *   <li>C —— 并发共享一条 channel 与每线程一条 channel 的对照（含落账时序与 confirm 序号账本）</li>
 * </ul>
 */
final class TopologyDemos {

    private TopologyDemos() {
    }

    static void run() throws Exception {
        DemoRunner.head("topology");
        DemoRunner.say("reset", "清理上一轮遗留的本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());
        partA();
        partB();
        partC();
        DemoRunner.say("reset", "本轮结束，再清一次本篇资源 %d 项", DemoRunner.mgmtPurgePrefix());
    }

    // ---------------------------------------------------------------- A

    private static void partA() throws Exception {
        DemoRunner.say("A", "--- A. 一个 connection 上开多条 channel；队列挂在 vhost 上 ---");
        Connection conn = DemoRunner.newConnection();
        try {
            Map<String, Object> sp = conn.getServerProperties();
            DemoRunner.say("A", "已连接 %s:%d vhost=%s user=%s",
                    DemoRunner.HOST, DemoRunner.AMQP_PORT, DemoRunner.VHOST, DemoRunner.USER);
            DemoRunner.say("A", "服务端自报 version=%s product=%s platform=%s",
                    sp.get("version"), sp.get("product"), sp.get("platform"));
            DemoRunner.say("A", "协商结果 channelMax=%d frameMax=%d heartbeat=%d",
                    conn.getChannelMax(), conn.getFrameMax(), conn.getHeartbeat());

            Channel ch1 = conn.createChannel();
            Channel ch2 = conn.createChannel();
            Channel ch3 = conn.createChannel();
            DemoRunner.say("A", "同一 connection 上开了 3 条 channel：#%d #%d #%d",
                    ch1.getChannelNumber(), ch2.getChannelNumber(), ch3.getChannelNumber());

            String q = DemoRunner.P + "topology.scope";
            DemoRunner.declareQueue(ch1, q, null);
            DemoRunner.say("A", "在 #%d 上声明队列 %s", ch1.getChannelNumber(), q);

            ch2.queueDeclarePassive(q);
            DemoRunner.say("A", "在 #%d 上被动声明同一队列 → 成功", ch2.getChannelNumber());

            ch3.queueDelete(q);
            DemoRunner.say("A", "在 #%d 上删除同一队列 → 成功", ch3.getChannelNumber());
            DemoRunner.say("A", "→ 队列是 vhost 级资源：哪条 channel 声明、哪条删除都不影响结论");

            DemoRunner.say("A2", "--- A2. 信道级协议错误 ---");
            DemoRunner.say("A2", "在 #%d 上被动声明一个不存在的队列", ch1.getChannelNumber());
            try {
                ch1.queueDeclarePassive(DemoRunner.P + "topology.no-such-queue");
                DemoRunner.say("A2", "意外：没有报错");
            } catch (Exception e) {
                DemoRunner.reportError("A2", e, conn, ch1);
            }

            Channel ch4 = conn.createChannel();
            String q2 = DemoRunner.P + "topology.after-error";
            DemoRunner.declareQueue(ch4, q2, null);
            DemoRunner.say("A2", "同一条 connection 上新建 #%d 并在其上声明 %s → 正常",
                    ch4.getChannelNumber(), q2);
            DemoRunner.say("A2", "→ 被关掉的是信道，不是连接；同一连接上重开一条 channel 继续用即可");

            ch4.queueDelete(q2);
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }

    // ---------------------------------------------------------------- B

    /** 一个探针就是「在一条干净连接上做一次手势」，看错误落在哪一级。 */
    private interface Action {
        void run(Connection conn, Channel ch) throws Exception;
    }

    private static void partB() {
        DemoRunner.say("B", "--- B. 一组手势，逐条看错误码落在哪一级 ---");

        probe("B1", "声明一个不存在的交换机类型",
                (conn, ch) -> ch.exchangeDeclare(DemoRunner.P + "bogus", "no-such-type"));

        probe("B2", "声明 x-queue-type=bogus",
                (conn, ch) -> ch.queueDeclare(DemoRunner.P + "b.qtype", true, false, false,
                        DemoRunner.args("x-queue-type", "bogus")));

        probe("B3", "发布时带上 immediate=true",
                (conn, ch) -> {
                    String q = DemoRunner.P + "b.immediate";
                    DemoRunner.declareQueue(ch, q, null);
                    ch.basicPublish("", q, true, true, null, DemoRunner.utf8("x"));
                });

        probe("B4", "basicQos(prefetchCount=1, global=true)",
                (conn, ch) -> ch.basicQos(0, 1, true));

        probe("B5", "把已经开着的 channel 号再开一次",
                (conn, ch) -> conn.createChannel(ch.getChannelNumber()));

        probe("B6", "手工往连接里塞一个「帧类型非法」的帧（故意违反协议）",
                (conn, ch) -> {
                    // 客户端默认返回的是 AutorecoveringConnection（自动恢复外壳），
                    // 它不继承 AMQConnection；要拿到底层连接得走 getDelegate()。
                    AMQConnection ac = (conn instanceof AutorecoveringConnection arc)
                            ? arc.getDelegate()
                            : (AMQConnection) conn;
                    ac.writeFrame(new Frame(99, ch.getChannelNumber(), new byte[]{1, 2, 3}));
                    ac.flush();
                });
    }

    /**
     * 跑一次手势，然后分别看「同步异常」与「异步上报」两条路径。
     *
     * <p>协议异常是异步的：broker 先关实体，客户端随后才收到 {@code channel.close} /
     * {@code connection.close} 方法。所以这里登记一个 ShutdownListener，再等一会儿。
     */
    private static void probe(String tag, String desc, Action action) {
        DemoRunner.say(tag, "--- %s ---", desc);
        Connection conn = null;
        Channel ch = null;
        final String[] asyncCause = {null};
        try {
            conn = DemoRunner.newConnection();
            conn.addShutdownListener(cause -> asyncCause[0] = DemoRunner.classify(cause));
            ch = conn.createChannel();
            action.run(conn, ch);
            DemoRunner.sleep(900);
            if (asyncCause[0] != null) {
                DemoRunner.say(tag, "（连接被关，异步上报）%s", asyncCause[0]);
            }
            DemoRunner.say(tag, "没有同步异常；之后 connection.isOpen=%s, channel.isOpen=%s",
                    conn.isOpen(), ch.isOpen());
        } catch (Exception e) {
            DemoRunner.reportError(tag, e, conn, ch);
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }

    // ---------------------------------------------------------------- C

    private static void partC() throws Exception {
        DemoRunner.say("C", "--- C. 并发：共享一条 channel 与每线程一条 channel ---");
        concurrencyProbe(true);
        concurrencyProbe(false);
        confirmSeqProbe();
    }

    private static void concurrencyProbe(boolean shared) throws Exception {
        String tag = shared ? "C1" : "C2";
        String label = shared
                ? "8 线程共享 1 条 channel"
                : "8 线程各开一条 channel（同一 connection）";
        final Connection conn = DemoRunner.newConnection();
        try {
            final String q = DemoRunner.P + "topology.conc." + (shared ? "shared" : "per-channel");
            Channel setup = conn.createChannel();
            DemoRunner.declareQueue(setup, q, null);
            setup.close();

            final int threads = 8;
            final int per = 200;
            final byte[] body = new byte[1024];
            final Channel sharedCh = shared ? conn.createChannel() : null;
            final List<Channel> perThread = Collections.synchronizedList(new ArrayList<>());
            final AtomicInteger ok = new AtomicInteger();
            final AtomicInteger err = new AtomicInteger();
            final List<String> samples = Collections.synchronizedList(new ArrayList<>());

            Thread[] ts = new Thread[threads];
            long t0 = System.nanoTime();
            for (int i = 0; i < threads; i++) {
                ts[i] = new Thread(() -> {
                    Channel own = null;
                    try {
                        if (!shared) {
                            own = conn.createChannel();
                            perThread.add(own);
                        }
                    } catch (Exception e) {
                        err.incrementAndGet();
                        samples.add("开信道失败: " + e);
                        return;
                    }
                    for (int j = 0; j < per; j++) {
                        try {
                            (shared ? sharedCh : own).basicPublish("", q, null, body);
                            ok.incrementAndGet();
                        } catch (Throwable t) {
                            err.incrementAndGet();
                            if (samples.size() < 4) {
                                samples.add(t.getClass().getSimpleName() + ": " + t.getMessage());
                            }
                        }
                    }
                }, "pub-" + i);
                ts[i].start();
            }
            for (Thread t : ts) {
                t.join();
            }
            long ms = (System.nanoTime() - t0) / 1_000_000;

            DemoRunner.say(tag, "%s：成功 %d / 失败 %d，耗时 %d ms（共 %d 次发布）",
                    label, ok.get(), err.get(), ms, threads * per);
            for (String s : samples) {
                DemoRunner.say(tag, "  异常样本: %s", s);
            }
            DemoRunner.say(tag, "connection.isOpen=%s，per-thread channel 还开着 %d 条",
                    conn.isOpen(), perThread.size());

            Channel chk = conn.createChannel();
            DemoRunner.say(tag, "发布结束后连续读队列深度（每 300ms 一次，此时信道都还开着）：");
            for (int i = 0; i < 6; i++) {
                DemoRunner.say(tag, "  t≈%4dms  深度=%d（期望 %d）",
                        i * 300, DemoRunner.depth(chk, q), threads * per);
                if (i < 5) {
                    DemoRunner.sleep(300);
                }
            }
            DemoRunner.state(tag, chk, q);
            chk.close();

            for (Channel c : perThread) {
                DemoRunner.closeQuietly(c);
            }
            if (!perThread.isEmpty()) {
                DemoRunner.sleep(500);
                Channel chk2 = conn.createChannel();
                DemoRunner.say(tag, "关掉全部 per-thread channel 之后再读：深度=%d",
                        DemoRunner.depth(chk2, q));
                chk2.close();
            }
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }

    /**
     * C3：并发共享一条 channel 时，publisher confirms 的序号账本。
     *
     * <p>被观察的对象是 {@code getNextPublishSeqNo()}：按官方用法，发出前先读这个值就知道
     * 「我这条消息会拿到哪个确认序号」。8 条线程共享一条 channel 时各自读一次，看读到的
     * 序号是否唯一。
     */
    private static void confirmSeqProbe() throws Exception {
        DemoRunner.say("C3", "--- C3. 共享 channel 时 publisher confirms 的序号账本 ---");
        final Connection conn = DemoRunner.newConnection();
        try {
            final String q = DemoRunner.P + "topology.confirm";
            Channel setup = conn.createChannel();
            DemoRunner.declareQueue(setup, q, null);
            setup.close();

            final Channel ch = conn.createChannel();
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

            final int threads = 8;
            final int per = 200;
            final List<Long> seqs = Collections.synchronizedList(new ArrayList<>());

            Thread[] ts = new Thread[threads];
            for (int i = 0; i < threads; i++) {
                ts[i] = new Thread(() -> {
                    for (int j = 0; j < per; j++) {
                        try {
                            seqs.add(ch.getNextPublishSeqNo());
                            ch.basicPublish("", q, null, new byte[64]);
                        } catch (Throwable ignored) {
                            // 这一支只关心序号账本，发布失败不在这里统计
                        }
                    }
                }, "seq-" + i);
                ts[i].start();
            }
            for (Thread t : ts) {
                t.join();
            }

            Set<Long> distinct = new HashSet<>(seqs);
            DemoRunner.say("C3", "8 线程共享 1 条 channel，各读一次 getNextPublishSeqNo() 再发布");
            DemoRunner.say("C3", "读到 %d 个序号，其中不同的只有 %d 个 → 重复 %d 个（唯一才应该是 %d）",
                    seqs.size(), distinct.size(), seqs.size() - distinct.size(), seqs.size());

            boolean confirmed;
            try {
                confirmed = ch.waitForConfirms(10_000);
            } catch (Exception e) {
                confirmed = false;
                DemoRunner.say("C3", "waitForConfirms 抛出: %s", e);
            }
            DemoRunner.say("C3", "waitForConfirms=%s；确认回调 ack=%d 次 nack=%d 次（发布共 %d 条）",
                    confirmed, acks.get(), nacks.get(), seqs.size());
        } finally {
            DemoRunner.closeQuietly(conn);
        }
    }
}
