package cn.renxinblog.gyd.c03;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消费者怎么拿到数据。对应文章第五节。
 *
 * <p>两个 demo 对照着看：并发消费用**多个线程**处理多个队列；顺序消费对**每个队列**
 * 只用一个线程且不允许交错。这就是「顺序消息的代价是把并发度锁到队列数」的实证。
 */
public final class ConsumerDemos {

    private ConsumerDemos() {
    }

    /** 并发消费：打印线程数、队列覆盖、以及流控与线程池的默认值。 */
    static void concurrent() throws Exception {
        DemoRunner.head("consume");
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("gyd_c03_consumer_concurrent");
        consumer.setNamesrvAddr(DemoRunner.namesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(DemoRunner.TOPIC_SEND, "*");

        Set<String> threads = ConcurrentHashMap.newKeySet();
        Set<Integer> queueIds = new TreeSet<>();
        AtomicInteger count = new AtomicInteger();

        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                threads.add(Thread.currentThread().getName());
                queueIds.add(message.getQueueId());
                count.incrementAndGet();
                DemoRunner.say("consume", "收到 queueId=%d body=%-10s 消费线程=%s",
                        message.getQueueId(), new String(message.getBody(), StandardCharsets.UTF_8),
                        Thread.currentThread().getName());
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        DemoRunner.say("consume", "默认流控: consumeThreadMin=%d consumeThreadMax=%d pullThresholdForQueue=%d 条 pullThresholdSizeForQueue=%d MB",
                consumer.getConsumeThreadMin(), consumer.getConsumeThreadMax(),
                consumer.getPullThresholdForQueue(), consumer.getPullThresholdSizeForQueue());
        DemoRunner.say("consume", "默认流控: consumeConcurrentlyMaxSpan=%d pullBatchSize=%d consumeMessageBatchMaxSize=%d maxReconsumeTimes=%d",
                consumer.getConsumeConcurrentlyMaxSpan(), consumer.getPullBatchSize(),
                consumer.getConsumeMessageBatchMaxSize(), consumer.getMaxReconsumeTimes());
        DemoRunner.say("consume", "位点持久化间隔 persistConsumerOffsetInterval=%d ms", consumer.getPersistConsumerOffsetInterval());

        DemoRunner.sleep(12_000);
        consumer.shutdown();
        DemoRunner.say("consume", "汇总: 收到 %d 条, 覆盖队列 %s, 实际用到消费线程 %d 个 %s",
                count.get(), queueIds, threads.size(), threads);
    }

    /**
     * 顺序消费：同一队列只有一个线程在处理，且前一条没返回就不会处理下一条。
     *
     * <p>监听器里刻意加 300ms 延迟，让「串行」可以从时间戳上直接看出来——
     * 如果并发，几条消息的区间会互相重叠。
     */
    static void orderly() throws Exception {
        DemoRunner.head("orderly");
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("gyd_c03_consumer_orderly");
        consumer.setNamesrvAddr(DemoRunner.namesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(DemoRunner.TOPIC_ORDER, "*");

        Map<Integer, Set<String>> threadsPerQueue = new ConcurrentHashMap<>();
        AtomicInteger count = new AtomicInteger();

        consumer.registerMessageListener((MessageListenerOrderly) (messages, context) -> {
            for (MessageExt message : messages) {
                int queueId = message.getQueueId();
                threadsPerQueue.computeIfAbsent(queueId, key -> ConcurrentHashMap.newKeySet())
                        .add(Thread.currentThread().getName());
                long startedAt = System.currentTimeMillis();
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                DemoRunner.say("orderly", "开始处理 queueId=%d body=%-8s 线程=%s 时刻=%d",
                        queueId, body, Thread.currentThread().getName(), startedAt);
                DemoRunner.sleep(300);
                DemoRunner.say("orderly", "处理完成 queueId=%d body=%-8s 时刻=%d  ← 与上一条区间不重叠即为串行",
                        queueId, body, System.currentTimeMillis());
                count.incrementAndGet();
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });

        consumer.start();
        DemoRunner.sleep(12_000);
        consumer.shutdown();

        Map<Integer, String> summary = new TreeMap<>();
        threadsPerQueue.forEach((queueId, threads) -> summary.put(queueId, threads.toString()));
        DemoRunner.say("orderly", "汇总: 处理 %d 条, 每个队列用到的线程 = %s", count.get(), summary);
    }
}
