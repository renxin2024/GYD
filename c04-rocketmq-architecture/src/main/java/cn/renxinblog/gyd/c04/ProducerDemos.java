package cn.renxinblog.gyd.c04;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 发送一条消息：从 send() 到 SendResult。对应文章第二节。
 *
 * <p>核心不是「怎么发」——API 就那么几个——而是「发送失败到底意味着什么」，
 * 那一部分由 {@link #sendStatusProbe()} 配合停/起从节点来观察。
 */
public final class ProducerDemos {

    private ProducerDemos() {
    }

    /** 同步 / 异步 / 单向 / 批量四种发送，并观察默认的轮询落点。 */
    static void syncAsyncOnewayBatch() throws Exception {
        DemoRunner.head("send");
        DefaultMQProducer producer = DemoRunner.producer("gyd_c03_producer_basic");
        try {
            DemoRunner.say("send", "默认值: sendMsgTimeout=%dms retryTimesWhenSendFailed=%d retryTimesWhenSendAsyncFailed=%d",
                    producer.getSendMsgTimeout(), producer.getRetryTimesWhenSendFailed(),
                    producer.getRetryTimesWhenSendAsyncFailed());
            DemoRunner.say("send", "默认值: retryAnotherBrokerWhenNotStoreOK=%s  ← false 表示 SendStatus 非 SEND_OK 也不换 broker 重试",
                    producer.isRetryAnotherBrokerWhenNotStoreOK());
            DemoRunner.say("send", "默认值: defaultTopicQueueNums=%d maxMessageSize=%d compressMsgBodyOverHowmuch=%d",
                    producer.getDefaultTopicQueueNums(), producer.getMaxMessageSize(),
                    producer.getCompressMsgBodyOverHowmuch());
            DemoRunner.say("send", "重试白名单 retryResponseCodes（命中才换队列重试）= %s", new TreeSet<>(producer.getRetryResponseCodes()));

            // 1) 同步 ×3：观察落点是否轮询
            Map<Integer, Integer> histogram = new TreeMap<>();
            for (int i = 0; i < 3; i++) {
                SendResult result = producer.send(new Message(DemoRunner.TOPIC_SEND, "TAG-A", DemoRunner.utf8("sync-" + i)));
                histogram.merge(result.getMessageQueue().getQueueId(), 1, Integer::sum);
                DemoRunner.say("send", "同步 #%d -> queueId=%d sendStatus=%s msgId=%s",
                        i, result.getMessageQueue().getQueueId(), result.getSendStatus(), result.getMsgId());
            }
            DemoRunner.say("send", "同步落点分布 queueId->条数 = %s", histogram);

            // 2) 异步：结果走回调，不占调用线程
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> asyncOutcome = new AtomicReference<>("(回调未触发)");
            producer.send(new Message(DemoRunner.TOPIC_SEND, "TAG-A", DemoRunner.utf8("async-0")), new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    asyncOutcome.set("queueId=" + sendResult.getMessageQueue().getQueueId() + " sendStatus=" + sendResult.getSendStatus());
                    latch.countDown();
                }

                @Override
                public void onException(Throwable e) {
                    asyncOutcome.set("异常=" + e.getClass().getSimpleName() + ": " + e.getMessage());
                    latch.countDown();
                }
            });
            latch.await(10, TimeUnit.SECONDS);
            DemoRunner.say("send", "异步回调 -> %s", asyncOutcome.get());

            // 3) 单向：只负责写出去，不等任何结果
            producer.sendOneway(new Message(DemoRunner.TOPIC_SEND, "TAG-A", DemoRunner.utf8("oneway-0")));
            DemoRunner.say("send", "单向发送 -> 无返回值（既不保证落盘，也没有重试机会）");

            // 4) 批量：整批一条 SendResult
            List<Message> batch = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                batch.add(new Message(DemoRunner.TOPIC_SEND, "TAG-A", DemoRunner.utf8("batch-" + i)));
            }
            SendResult batchResult = producer.send(batch);
            DemoRunner.say("send", "批量 ×3 -> 单条 SendResult queueId=%d sendStatus=%s",
                    batchResult.getMessageQueue().getQueueId(), batchResult.getSendStatus());
        } finally {
            producer.shutdown();
        }
    }

    /**
     * 顺序发送：把消息固定投到同一个队列。
     *
     * <p>这一节要讲清楚的是「顺序消息的串行落在哪一层」——发送端只负责落同一队列，
     * 真正的串行在消费端（见 orderly demo）。
     */
    static void ordered() throws Exception {
        DemoRunner.head("ordered");
        DefaultMQProducer producer = DemoRunner.producer("gyd_c03_producer_order");
        try {
            final int targetQueueId = 2;
            for (int i = 0; i < 6; i++) {
                Message message = new Message(DemoRunner.TOPIC_ORDER, "TAG-A", DemoRunner.utf8("order-" + i));
                SendResult result = producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> queues, Message msg, Object arg) {
                        return queues.get((Integer) arg);
                    }
                }, targetQueueId);
                DemoRunner.say("ordered", "顺序 #%d 指定 queueId=%d 实际 queueId=%d",
                        i, targetQueueId, result.getMessageQueue().getQueueId());
            }
            DemoRunner.say("ordered", "发送端只做到「落同一队列」；串行由消费端的 orderly 监听器保证");
        } finally {
            producer.shutdown();
        }
    }

    /**
     * 单次同步发送，打印 SendStatus 与耗时。
     *
     * <p>这个 demo 本身不做什么，价值在于**配合集群状态变化重复运行**：
     * 起从节点时是 SEND_OK，停掉从节点后（broker 是 SYNC_MASTER）会变成
     * SLAVE_NOT_AVAILABLE —— 这就是「生产者看到失败 ≠ 消息没写进去」的那一类。
     */
    static void sendStatusProbe() throws Exception {
        DemoRunner.head("failmode");
        DefaultMQProducer producer = DemoRunner.producer("gyd_c03_producer_failmode");
        try {
            DemoRunner.say("failmode", "retryAnotherBrokerWhenNotStoreOK=%s retryTimesWhenSendFailed=%d",
                    producer.isRetryAnotherBrokerWhenNotStoreOK(), producer.getRetryTimesWhenSendFailed());
            long startedAt = System.currentTimeMillis();
            SendResult result = producer.send(new Message(DemoRunner.TOPIC_SEND, "TAG-A", DemoRunner.utf8("failmode-probe")));
            long cost = System.currentTimeMillis() - startedAt;
            DemoRunner.say("failmode", "sendStatus=%s 耗时=%dms queueId=%d",
                    result.getSendStatus(), cost, result.getMessageQueue().getQueueId());
            DemoRunner.say("failmode", "注意：这里没有抛异常。抛不抛、重不重试，取决于响应码在不在白名单里");
        } finally {
            producer.shutdown();
        }
    }
}
