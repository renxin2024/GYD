package cn.renxinblog.gyd.c01.producer;

import cn.renxinblog.gyd.c01.config.RabbitMqConfig;
import cn.renxinblog.gyd.c01.message.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * 订单消息生产者。
 *
 * 演示「生产者怎么确认消息真的进了 Broker」，以及确认之外还差什么：
 * <ul>
 *   <li>{@link #send} —— 异步投递，不等回执，结果由 confirm 回调异步给出；</li>
 *   <li>{@link #sendAndWaitConfirm} —— 同步投递，阻塞等 Broker 回执，只有确认才返回；</li>
 *   <li>{@link #sendUnroutable} —— 能进交换机但进不了队列（confirm 仍然 ack）；</li>
 *   <li>{@link #sendPersistenceProbe} —— 对照投递持久化 / 非持久化消息，验证「ack ≠ 落盘」。</li>
 * </ul>
 */
@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final RabbitTemplate rabbitTemplate;

    public OrderProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /** 异步投递：发完即返回，回执由 {@code setConfirmCallback} 异步打印。 */
    public void send(OrderMessage message) {
        CorrelationData correlationData = new CorrelationData(message.orderId());
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.ORDER_EXCHANGE,
                RabbitMqConfig.ORDER_ROUTING_KEY,
                message,
                correlationData);
        log.info("[producer] 已投递订单消息: orderId={}", message.orderId());
    }

    /** 同步投递：阻塞等 Broker 回执，未确认就抛异常，由调用方决定是否重发。 */
    public void sendAndWaitConfirm(OrderMessage message) throws Exception {
        CorrelationData correlationData = new CorrelationData(message.orderId());
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.ORDER_EXCHANGE,
                RabbitMqConfig.ORDER_ROUTING_KEY,
                message,
                correlationData);

        CorrelationData.Confirm confirm = correlationData.getFuture().get(3, TimeUnit.SECONDS);
        // Spring AMQP 4 起 Confirm 是 record，访问器为 ack()/reason()
        // （旧的 isAck()/getReason() 已标记待删除）
        if (!confirm.ack()) {
            throw new IllegalStateException("Broker 未确认消息: " + confirm.reason());
        }
        // 不可路由时 confirm 依然 ack，此时消息被 return 退回、并未进队列。
        // 只查 ack 会把「路由不到任何队列」当成「发送成功」，必须再查 return。
        if (correlationData.getReturned() != null) {
            throw new IllegalStateException(
                    "消息未能路由到队列: " + correlationData.getReturned().getReplyText());
        }
        log.info("[producer] Broker 已确认接收: orderId={}", message.orderId());
    }

    /**
     * 批量异步投递。
     *
     * 用于 prefetch 对比实验：一次性灌入足够多的消息，让消费端有稳定的积压可观测。
     */
    public void sendBurst(String prefix, int count) {
        for (int i = 0; i < count; i++) {
            OrderMessage message = new OrderMessage(
                    prefix + "-" + i, "u-0001", 100L, Instant.now().toString());
            rabbitTemplate.convertAndSend(
                    RabbitMqConfig.ORDER_EXCHANGE,
                    RabbitMqConfig.ORDER_ROUTING_KEY,
                    message,
                    new CorrelationData(message.orderId()));
        }
        log.info("[producer] 已批量投递 {} 条消息: prefix={}", count, prefix);
    }

    /**
     * 故意发到一个没有任何队列绑定的路由键。
     *
     * 消息能进交换机（confirm 依然会 ack），但路由不到队列，
     * 在 mandatory=true 下会触发 return 回调；若不处理该回调，消息就此静默消失。
     */
    public void sendUnroutable(OrderMessage message) {
        CorrelationData correlationData = new CorrelationData(message.orderId());
        rabbitTemplate.convertAndSend(
                RabbitMqConfig.ORDER_EXCHANGE,
                "order.nowhere",
                message,
                correlationData);
        log.info("[producer] 已投递到无人绑定的路由键: orderId={}", message.orderId());
    }

    /**
     * 持久化探针：投递到没有消费者的队列，并指定是否标记为持久化消息。
     *
     * 走默认交换机（exchange 传空串 + routingKey 等于队列名），避免再声明一套绑定。
     *
     * @param persistent true 为持久化消息（deliveryMode=2），false 为非持久化（deliveryMode=1）
     */
    public void sendPersistenceProbe(String id, boolean persistent) {
        rabbitTemplate.convertAndSend("", RabbitMqConfig.PERSIST_PROBE_QUEUE, "probe-" + id, message -> {
            message.getMessageProperties().setMessageId(id);
            message.getMessageProperties().setDeliveryMode(
                    persistent ? MessageDeliveryMode.PERSISTENT : MessageDeliveryMode.NON_PERSISTENT);
            return message;
        }, new CorrelationData(id));
        log.info("[producer] 已投递持久化探针: id={}, deliveryMode={}",
                id, persistent ? "PERSISTENT(2)" : "NON_PERSISTENT(1)");
    }
}
