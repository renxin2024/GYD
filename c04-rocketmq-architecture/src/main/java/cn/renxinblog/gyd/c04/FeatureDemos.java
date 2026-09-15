package cn.renxinblog.gyd.c04;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 三个「功能」demo：过滤、定时、事务。对应文章第六、七、八节。
 */
public final class FeatureDemos {

    private FeatureDemos() {
    }

    /** tag 过滤：订 TAG-A 的消费者不该看到 TAG-B 的消息。 */
    static void filterByTag() throws Exception {
        DemoRunner.head("filter-tag");
        DefaultMQProducer producer = DemoRunner.producer("gyd_c03_producer_filter_tag");
        try {
            for (int i = 0; i < 3; i++) {
                producer.send(new Message(DemoRunner.TOPIC_FILTER, "TAG-A", DemoRunner.utf8("A-" + i)));
                producer.send(new Message(DemoRunner.TOPIC_FILTER, "TAG-B", DemoRunner.utf8("B-" + i)));
            }
            DemoRunner.say("filter-tag", "已发送 TAG-A ×3 与 TAG-B ×3");
        } finally {
            producer.shutdown();
        }

        List<String> received = collect(new DefaultMQPushConsumer("gyd_c03_consumer_filter_tag"),
                DemoRunner.TOPIC_FILTER, MessageSelector.byTag("TAG-A"), 10_000);
        DemoRunner.say("filter-tag", "订阅 TAG-A 实际收到 %d 条: %s", received.size(), received);
        DemoRunner.say("filter-tag", "TAG-B 的三条从未离开 broker：过滤发生在投递之前，不是客户端收到后再丢");
        DemoRunner.say("filter-tag", "依据：tag 的 hashcode 就存在 ConsumeQueue 的 20 字节定长条目里，只读索引即可判断");
    }

    /**
     * SQL92 过滤。
     *
     * <p>这条 demo 的重点是**它会先失败**：4.x 的 broker 默认 {@code enablePropertyFilter=false}，
     * 此时订阅 SQL92 会在 start() 上直接抛 MQClientException。打开开关并重启 broker 后才可用。
     * 这个「失败—打开开关—可用」的过程本身就是文章要讲的失败排查。
     */
    static void filterBySql() throws Exception {
        DemoRunner.head("filter-sql");
        DefaultMQProducer producer = DemoRunner.producer("gyd_c03_producer_filter_sql");
        try {
            for (int i = 1; i <= 10; i++) {
                Message message = new Message(DemoRunner.TOPIC_FILTER, "SQL", DemoRunner.utf8("a=" + i));
                message.putUserProperty("a", String.valueOf(i));
                producer.send(message);
            }
            DemoRunner.say("filter-sql", "已发送 10 条，用户属性 a=1..10，过滤条件是 a > 5");
        } finally {
            producer.shutdown();
        }

        try {
            List<String> received = collect(new DefaultMQPushConsumer("gyd_c03_consumer_filter_sql"),
                    DemoRunner.TOPIC_FILTER, MessageSelector.bySql("a > 5"), 10_000);
            DemoRunner.say("filter-sql", "收到 %d 条: %s", received.size(), received);
            DemoRunner.say("filter-sql", "与 tag 的差别：SQL92 要读消息属性，属性在 CommitLog 里，所以发生在消息取出之后");
        } catch (Exception e) {
            DemoRunner.say("filter-sql", "订阅失败: %s: %s", e.getClass().getName(), e.getMessage());
            Throwable cause = e.getCause();
            while (cause != null) {
                DemoRunner.say("filter-sql", "  cause: %s: %s", cause.getClass().getName(), cause.getMessage());
                cause = cause.getCause();
            }
            DemoRunner.say("filter-sql", "预期结论：broker 的 enablePropertyFilter 默认 false，SQL92 需要先在 broker 打开并重启");
        }
    }

    /**
     * 定时消息：只能选级表里的档位，实测延迟与标称延迟的差就是精度误差。
     *
     * <p>同时探一个**不在级表里**的级别，看它是被拒绝还是被兜底——这一点文档没写清，
     * 只能实测。
     */
    static void delay() throws Exception {
        DemoRunner.head("delay");
        Map<String, Long> sentAt = new ConcurrentHashMap<>();
        Map<String, Long> receivedAt = new ConcurrentHashMap<>();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("gyd_c03_consumer_delay");
        consumer.setNamesrvAddr(DemoRunner.namesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(DemoRunner.TOPIC_DELAY, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                receivedAt.put(body, System.currentTimeMillis());
                DemoRunner.say("delay", "收到 %-12s 时刻=%d", body, System.currentTimeMillis());
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        DemoRunner.sleep(2_000);

        DefaultMQProducer producer = DemoRunner.producer("gyd_c03_producer_delay");
        try {
            int[] levels = {1, 3, 4};
            long[] nominal = {1_000, 10_000, 30_000};
            for (int i = 0; i < levels.length; i++) {
                String body = "level-" + levels[i];
                Message message = new Message(DemoRunner.TOPIC_DELAY, DemoRunner.utf8(body));
                message.setDelayTimeLevel(levels[i]);
                sentAt.put(body, System.currentTimeMillis());
                SendResult result = producer.send(message);
                DemoRunner.say("delay", "发送 %-12s 标称延迟=%6dms sendStatus=%s", body, nominal[i], result.getSendStatus());
            }

            for (int level : new int[]{19, 99}) {
                String body = "level-" + level;
                Message message = new Message(DemoRunner.TOPIC_DELAY, DemoRunner.utf8(body));
                message.setDelayTimeLevel(level);
                sentAt.put(body, System.currentTimeMillis());
                try {
                    SendResult result = producer.send(message);
                    DemoRunner.say("delay", "发送 %-12s (不在级表内) sendStatus=%s", body, result.getSendStatus());
                } catch (Exception e) {
                    DemoRunner.say("delay", "发送 %-12s (不在级表内) 抛异常: %s: %s",
                            body, e.getClass().getSimpleName(), e.getMessage());
                }
            }
        } finally {
            producer.shutdown();
        }

        DemoRunner.sleep(40_000);
        consumer.shutdown();

        DemoRunner.say("delay", "===== 实测延迟 =====");
        sentAt.keySet().stream().sorted().forEach(body -> {
            Long sent = sentAt.get(body);
            Long received = receivedAt.get(body);
            DemoRunner.say("delay", "%-12s 实测延迟=%s", body,
                    received == null ? "未收到（可能被丢弃）" : (received - sent) + "ms");
        });
    }

    /**
     * 事务消息：半消息 + 本地事务 + 回查。
     *
     * <p>三条消息覆盖三种本地事务结果：commit / rollback / UNKNOW（等回查）。
     * 生产者在回查完成前必须**保持存活**，否则回查拿不到答案——这一点也要在输出里体现。
     */
    static void transaction() throws Exception {
        DemoRunner.head("tx");
        long startedAt = System.currentTimeMillis();
        List<String> timeline = Collections.synchronizedList(new ArrayList<>());
        List<String> arrived = Collections.synchronizedList(new ArrayList<>());

        // 消费端先起来，用来观察「半消息对消费者不可见」
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("gyd_c03_consumer_tx");
        consumer.setNamesrvAddr(DemoRunner.namesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(DemoRunner.TOPIC_TX, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                arrived.add(body);
                DemoRunner.say("tx", "消费者可见 t+%dms %s", System.currentTimeMillis() - startedAt, body);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        DemoRunner.sleep(2_000);

        TransactionMQProducer producer = new TransactionMQProducer("gyd_c03_tx_producer");
        producer.setNamesrvAddr(DemoRunner.namesrvAddr());
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object arg) {
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                LocalTransactionState state = switch (body) {
                    case "tx-commit" -> LocalTransactionState.COMMIT_MESSAGE;
                    case "tx-rollback" -> LocalTransactionState.ROLLBACK_MESSAGE;
                    default -> LocalTransactionState.UNKNOW;
                };
                timeline.add(String.format("t+%dms executeLocalTransaction(%s) -> %s",
                        System.currentTimeMillis() - startedAt, body, state));
                return state;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt message) {
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                timeline.add(String.format("t+%dms checkLocalTransaction(%s) 被回查 -> COMMIT_MESSAGE",
                        System.currentTimeMillis() - startedAt, body));
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();

        try {
            for (String body : List.of("tx-commit", "tx-rollback", "tx-unknown")) {
                TransactionSendResult result = producer.sendMessageInTransaction(
                        new Message(DemoRunner.TOPIC_TX, DemoRunner.utf8(body)), null);
                DemoRunner.say("tx", "发送 %-12s localTransactionState=%-16s sendStatus=%s",
                        body, result.getLocalTransactionState(), result.getSendStatus());
            }

            DemoRunner.sleep(5_000);
            DemoRunner.say("tx", "发送后 5s，消费者已可见: %s", new ArrayList<>(arrived));
            DemoRunner.say("tx", "此刻 tx-unknown 是半消息，对消费者不可见；tx-rollback 已被丢弃");

            DemoRunner.say("tx", "等待 broker 回查（transactionCheckInterval 默认 60000ms）...");
            DemoRunner.sleep(80_000);

            DemoRunner.say("tx", "回查窗口后，消费者已可见: %s", new ArrayList<>(arrived));
            DemoRunner.say("tx", "===== 事务监听器调用轨迹 =====");
            timeline.forEach(line -> DemoRunner.say("tx", "%s", line));
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    /**
     * 起一个消费者收 {@code millis} 毫秒，返回收到的消息体。
     *
     * <p>订阅表达式由调用方显式给出（tag 或 SQL92），不在这里猜。
     */
    private static List<String> collect(DefaultMQPushConsumer consumer, String topic, MessageSelector selector, long millis)
            throws Exception {
        List<String> received = Collections.synchronizedList(new ArrayList<>());
        consumer.setNamesrvAddr(DemoRunner.namesrvAddr());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, selector);
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            for (MessageExt message : messages) {
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                received.add(body);
                DemoRunner.say("collect", "收到 %s", body);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        DemoRunner.sleep(millis);
        consumer.shutdown();
        return received;
    }
}
