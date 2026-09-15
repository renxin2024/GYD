package cn.renxinblog.gyd.c02.consumer;

import cn.renxinblog.gyd.c02.config.RabbitMqConfig;
import cn.renxinblog.gyd.c02.message.OrderMessage;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 订单消息消费者。
 *
 * 采用「先处理业务、成功后手动 ack」的顺序：处理成功才确认，失败则交给重试链路。
 * 这个顺序决定了消费端在「不丢」和「不重」之间的取舍。
 *
 * 重试次数不是应用自己数的，而是读 broker 写在 {@code x-death} 头里的死信记录——
 * 每被死信一次，broker 就累加对应记录的 count。这是「重试了几次」的权威来源：
 * 应用自己维护计数器，进程重启后就归零了，而头部跟着消息走。
 *
 * 实现上从 {@code @Headers} 整表取，而不是声明 {@code @Header("x-death")} 参数：
 * 后者实测拿不到头部值（绑定到的是原始 Message 对象），x-death 是嵌套的
 * {@code List<Map<String,?>>}，从整表里自己取更直接。
 */
@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    /** 首次消费之外最多重试几次，用尽后进最终死信队列。 */
    public static final int MAX_RETRY = 3;

    private final RabbitTemplate rabbitTemplate;

    /** 慢消费模拟耗时（毫秒），仅用于 prefetch 实验；默认 0 表示不额外等待。 */
    private final long slowProcessingMs;

    public OrderConsumer(RabbitTemplate rabbitTemplate,
                         @Value("${gyd.c02.slow-processing-ms:0}") long slowProcessingMs) {
        this.rabbitTemplate = rabbitTemplate;
        this.slowProcessingMs = slowProcessingMs;
    }

    @RabbitListener(queues = RabbitMqConfig.ORDER_QUEUE)
    public void onOrderCreated(OrderMessage message,
                               Channel channel,
                               @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
                               @Headers Map<String, Object> headers) throws IOException {
        // 已经因「被拒绝」死信过几次，就是重试了几次；本次投递是第 N+1 次处理
        int retriesDone = countRejected(headers.get("x-death"), RabbitMqConfig.ORDER_QUEUE);
        int attempt = retriesDone + 1;

        try {
            // orderId 含 "slow" 时故意慢处理，用来放大 prefetch 的效果
            if (message.orderId() != null && message.orderId().contains("slow") && slowProcessingMs > 0) {
                Thread.sleep(slowProcessingMs);
            }

            // orderId 含 "fail" 时故意抛异常，用来演示重试与死信链路
            if (message.orderId() != null && message.orderId().contains("fail")) {
                throw new IllegalStateException("故意失败：用于演示重试与死信链路");
            }

            log.info("[consumer] 订单处理成功: orderId={}, 第 {} 次投递, x-death={}",
                    message.orderId(), attempt, describe(headers.get("x-death")));

            // 业务处理成功后位点才前进
            channel.basicAck(deliveryTag, false);
        } catch (Exception ex) {
            if (retriesDone < MAX_RETRY) {
                log.warn("[consumer] 处理失败，{}ms 后重试（第 {}/{} 次）: orderId={}, x-death={}",
                        RabbitMqConfig.RETRY_TTL_MS, retriesDone + 1, MAX_RETRY,
                        message.orderId(), describe(headers.get("x-death")));
                // requeue=false：不重回原队列（否则原地死循环），交给重试交换机等 TTL
                channel.basicNack(deliveryTag, false, false);
            } else {
                log.error("[consumer] 重试 {} 次仍失败，转入死信队列: orderId={}, 累计投递 {} 次",
                        MAX_RETRY, message.orderId(), attempt);
                // 用尽重试：显式投到最终死信交换机，确认转发成功才 ack 掉原消息。
                // 这里不能再 nack——本队列的死信出口是重试交换机，会再次进入重试循环。
                forwardToDeadLetter(message, channel, deliveryTag, attempt);
            }
        }
    }

    /**
     * 用尽重试后的收尾：把消息显式投到最终死信交换机，确认转发成功之后才 ack 原消息。
     *
     * {@code convertAndSend} 返回不代表死信队列已经收到消息；如果随后路由失败、连接
     * 故障或 Broker 拒绝，原消息却已经 ack，就失去了恢复来源。所以这里同步等
     * {@link CorrelationData} 的 future，并同时检查 return——不可路由时 confirm 也会 ack，
     * 只查 ack 会把「转发进了死胡同」当成「转发成功」。
     *
     * 转发失败时不 ack，改为 requeue 回原队列，等下一次投递再试转发，消息不丢。
     */
    private void forwardToDeadLetter(OrderMessage message, Channel channel,
                                     long deliveryTag, int attempt) throws IOException {
        CorrelationData correlationData = new CorrelationData();
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.DEAD_LETTER_EXCHANGE,
                RabbitMqConfig.DEAD_LETTER_ROUTING_KEY,
                message,
                m -> {
                    m.getMessageProperties().setHeader("x-gyd-attempts", attempt);
                    return m;
                },
                correlationData);
        try {
            CorrelationData.Confirm confirm = correlationData.getFuture().get(5, TimeUnit.SECONDS);
            if (confirm.ack() && correlationData.getReturned() == null) {
                channel.basicAck(deliveryTag, false);
            } else {
                log.error("[consumer] 死信转发未确认（ack={}, returned={}），消息重投: orderId={}",
                        confirm.ack(), correlationData.getReturned() != null, message.orderId());
                channel.basicNack(deliveryTag, false, true);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.basicNack(deliveryTag, false, true);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            log.error("[consumer] 等待死信确认异常，消息重投: orderId={}", message.orderId(), e);
            channel.basicNack(deliveryTag, false, true);
        }
    }

    /**
     * 从 broker 写入的 x-death 头里数出「因被拒绝而死信」的次数。
     *
     * x-death 是数组，每项形如
     * { queue: gyd.c02.order.queue, reason: rejected, count: 2, exchange: ..., routing-keys: [...] }。
     * 只统计 reason=rejected：reason=expired 那条来自重试队列的 TTL 到期，
     * 同一次失败会在两处各留一条记录，全算上就重复计数了。
     */
    private static int countRejected(Object xDeath, String queueName) {
        if (!(xDeath instanceof List<?> records)) {
            return 0;
        }
        int total = 0;
        for (Object record : records) {
            if (record instanceof Map<?, ?> entry
                    && queueName.equals(entry.get("queue"))
                    && "rejected".equals(entry.get("reason"))
                    && entry.get("count") instanceof Number count) {
                total += count.intValue();
            }
        }
        return total;
    }

    /** 把 x-death 压成一行易读的摘要，便于在日志里直接看到重试轨迹。 */
    private static String describe(Object xDeath) {
        if (!(xDeath instanceof List<?> records)) {
            return "(无)";
        }
        StringBuilder sb = new StringBuilder();
        for (Object record : records) {
            if (record instanceof Map<?, ?> entry) {
                if (sb.length() > 0) {
                    sb.append(" + ");
                }
                sb.append(entry.get("queue")).append('/').append(entry.get("reason"))
                        .append(" ×").append(entry.get("count"));
            }
        }
        return sb.toString();
    }
}
