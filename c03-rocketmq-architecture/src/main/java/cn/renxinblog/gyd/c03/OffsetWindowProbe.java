package cn.renxinblog.gyd.c03;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 位点窗口探针：验证「客户端把位点缓存在内存里、按 {@code persistConsumerOffsetInterval}
 * （默认 5 秒）批量上报」这件事在进程被 kill 之后会怎样。
 *
 * <p>要证明的不是「5 秒是个配置值」（那个打印一下就有），而是这条因果链：
 * <b>进程在两次上报之间被强杀 → 已消费但未上报的那一段位点丢失 → 重启后重复消费</b>。
 *
 * <p>所以实验设计成对照组，唯一变量是「有没有给客户端机会上报位点」：
 * <ul>
 *   <li>A 组：{@code consume 0}（永不自动退出），外部在第一条上报之前 {@code kill -9}；</li>
 *   <li>B 组：{@code consume <总条数>}，消费完自动 {@code shutdown()}（shutdown 会 flush 位点）。</li>
 * </ul>
 * 两组都重启同 group 的消费者，看它从哪个 offset 开始。
 *
 * <pre>
 * 用法：
 *   run --args="offsetprobe produce 20"   # 发 20 条，body 里带序号
 *   run --args="offsetprobe consume 0"    # 不自动退出，供外部 kill -9
 *   run --args="offsetprobe consume 20"   # 消费满 20 条后 shutdown（B 组）
 * </pre>
 *
 * <p>本探针默认单队列主题（{@code -w 1}）并单线程消费，好让「消费到第几条」是确定的时间序列。
 */
public final class OffsetWindowProbe {

    /** 与正文 demo 分开的主题，避免污染其它实验的位点。 */
    public static final String TOPIC_OFFSET = "gyd-c03-offset";

    /**
     * 独立的消费组，保证 broker 上没有历史位点。可用环境变量 {@code RMQ_OFFSET_GROUP} 覆盖，
     * 好让「强杀」与「优雅退出」两组各用各的位点做对照。
     */
    private static final String GROUP = groupName();

    private static String groupName() {
        String fromEnv = System.getenv("RMQ_OFFSET_GROUP");
        return (fromEnv == null || fromEnv.isBlank()) ? "gyd-c03-offset-probe" : fromEnv;
    }

    /**
     * 每条消息的消费耗时。选 300ms 是为了让「消费到第 4 条」稳定发生在客户端第一次
     * 位点上报（{@code persistConsumerOffsetInterval} 默认 5000ms）之前——A 组要强杀的
     * 正是这个窗口，慢了会擦边。
     */
    private static final long PER_MESSAGE_MS = 300L;

    /** 消费满 expected 条后，主线程还要等一小会儿才 shutdown，避免卡在最后一批。 */
    private static final long TAIL_WAIT_MS = 3000L;

    /** consume 模式下主线程的最长驻留时间（外部 kill 的窗口）。 */
    private static final long MAX_IDLE_MS = 180_000L;

    private OffsetWindowProbe() {
    }

    public static void run(String[] args) throws Exception {
        String mode = args.length > 1 ? args[1] : "help";
        int arg = args.length > 2 ? Integer.parseInt(args[2]) : 0;
        switch (mode) {
            case "produce" -> produce(arg);
            case "consume" -> consume(arg);
            default -> DemoRunner.say("offset-probe", "用法: offsetprobe <produce|consume> <条数>");
        }
    }

    private static void produce(int count) throws Exception {
        DefaultMQProducerHolder holder = new DefaultMQProducerHolder();
        try {
            for (int i = 0; i < count; i++) {
                Message msg = new Message(TOPIC_OFFSET, "OFFSET", DemoRunner.utf8("off-" + i));
                msg.setKeys("off-" + i);
                holder.producer.send(msg);
            }
            DemoRunner.say("offset-probe", "已发送 %d 条到 %s", count, TOPIC_OFFSET);
        } finally {
            holder.producer.shutdown();
        }
    }

    private static void consume(int expected) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(GROUP);
        consumer.setNamesrvAddr(DemoRunner.namesrvAddr());
        // 位点不存在时从最小位点开始 —— 这正是重启后能观察到「重复消费」的前提。
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.subscribe(TOPIC_OFFSET, "*");

        long pid = ProcessHandle.current().pid();
        AtomicInteger seen = new AtomicInteger();
        // 计数恒为 1：expected <= 0（供外部 kill 的那一组）时 countDown 永不发生，
        // await 会一直阻塞到 MAX_IDLE_MS —— 这才是「等外部强杀」该有的语义。
        // 早先用 new CountDownLatch(expected > 0 ? 1 : 0) 是个 bug：计数为 0 时 await 立即返回，
        // 进程会自己优雅退出，把 A 组测成了 B 组。
        CountDownLatch done = new CountDownLatch(1);

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt m : msgs) {
                int n = seen.incrementAndGet();
                DemoRunner.say("offset-probe", "RECV pid=%d q=%d offset=%d n=%d body=%s",
                        pid, m.getQueueId(), m.getQueueOffset(), n,
                        new String(m.getBody(), StandardCharsets.UTF_8));
                DemoRunner.sleep(PER_MESSAGE_MS);
            }
            if (expected > 0 && seen.get() >= expected) {
                done.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        DemoRunner.say("offset-probe", "consumer 启动 pid=%d group=%s topic=%s 自动退出阈值=%s",
                pid, GROUP, TOPIC_OFFSET, expected > 0 ? String.valueOf(expected) : "（无，等外部 kill）");

        boolean finished = done.await(MAX_IDLE_MS, TimeUnit.MILLISECONDS);
        DemoRunner.say("offset-probe", "本轮共消费 %d 条，%s", seen.get(),
                finished ? "已达标，准备 shutdown（shutdown 会上报位点）" : "未达标或无可等待阈值");
        DemoRunner.sleep(TAIL_WAIT_MS);
        consumer.shutdown();
        DemoRunner.say("offset-probe", "consumer 已 shutdown pid=%d", pid);
    }

    /** 只为了让 produce 分支的 try/finally 好读，避免在 run 里层层嵌套。 */
    private static final class DefaultMQProducerHolder {
        final org.apache.rocketmq.client.producer.DefaultMQProducer producer;

        DefaultMQProducerHolder() throws Exception {
            this.producer = DemoRunner.producer("gyd-c03-offset-probe-producer");
        }
    }
}
