package cn.renxinblog.gyd.c01.config;

import cn.renxinblog.gyd.c01.message.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.DefaultJacksonJavaTypeMapper;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ 拓扑声明与收发两端的可靠性配置。
 *
 * <pre>
 * 正常路径：
 *   producer → order.exchange --order.created--&gt; order.queue → consumer --ack--&gt; 完成
 *
 * 失败路径（broker 驱动，靠死信机制自动延迟）：
 *   consumer --nack(requeue=false)--&gt; order.retry.exchange --order.retry--&gt; order.retry.queue
 *          --T TTL 到期--&gt; order.exchange --order.created--&gt; order.queue   （回到业务队列重试）
 *
 * 重试用尽（应用驱动，显式投递）：
 *   consumer --publish--&gt; order.dlx --order.dead--&gt; order.dlq                （等待人工 / 定时任务）
 * </pre>
 *
 * 注意重试那一跳必须由 broker 的死信机制完成：只有死信过的消息才会被 broker
 * 追加 {@code x-death} 头，消费者正是靠它数出「已经重试了几次」。
 * 如果改成应用自己重发（publish），新消息不带 x-death，计数就断了。
 */
@Configuration
public class RabbitMqConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqConfig.class);

    /** 业务交换机（direct） */
    public static final String ORDER_EXCHANGE = "gyd.c01.order.exchange";
    /** 业务队列 */
    public static final String ORDER_QUEUE = "gyd.c01.order.queue";
    /** 业务路由键 */
    public static final String ORDER_ROUTING_KEY = "order.created";

    /** 重试交换机：业务队列的死信出口 */
    public static final String RETRY_EXCHANGE = "gyd.c01.order.retry.exchange";
    /** 重试队列：消息在此等待 TTL 到期，再被重新投回业务队列 */
    public static final String RETRY_QUEUE = "gyd.c01.order.retry.queue";
    /** 重试路由键 */
    public static final String RETRY_ROUTING_KEY = "order.retry";
    /**
     * 重试延迟（毫秒）。
     *
     * 注意这是「队列级 TTL」：消息从队头开始过期，因此它只适合重试间隔固定的场景。
     * 若要每条消息各自的延迟（如 1s / 10s / 60s 递增退避），队列级 TTL 做不到，
     * 需要改用延迟插件或每级一个队列。
     */
    public static final int RETRY_TTL_MS = 2000;

    /** 最终死信交换机 */
    public static final String DEAD_LETTER_EXCHANGE = "gyd.c01.order.dlx";
    /** 最终死信队列 */
    public static final String DEAD_LETTER_QUEUE = "gyd.c01.order.dlq";
    /** 最终死信路由键 */
    public static final String DEAD_LETTER_ROUTING_KEY = "order.dead";

    /** 持久化探针队列：故意不挂消费者，用来观察消息能否挺过 broker 重启 */
    public static final String PERSIST_PROBE_QUEUE = "gyd.c01.persist.probe";

    @Bean
    public MessageConverter jsonMessageConverter() {
        // Spring Boot 4 默认使用 Jackson 3（tools.jackson），对应的转换器是 JacksonJsonMessageConverter。
        // 旧的 Jackson2JsonMessageConverter 依赖 com.fasterxml.jackson.databind，
        // 而 Boot 4 的类路径上只剩 com.fasterxml.jackson 的注解包，没有 databind。
        JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter();

        // 转换器默认只信任 java.util 和 java.lang（防止反序列化攻击），
        // 自定义消息类必须显式加入白名单，否则消费侧会抛
        // 「The class ... is not in the trusted packages」。这里按包名授权，不要图省事写成 "*"。
        DefaultJacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper();
        typeMapper.setTrustedPackages(OrderMessage.class.getPackageName());
        converter.setJavaTypeMapper(typeMapper);

        return converter;
    }

    @Bean
    public DirectExchange orderExchange() {
        return ExchangeBuilder.directExchange(ORDER_EXCHANGE).durable(true).build();
    }

    @Bean
    public Queue orderQueue() {
        return QueueBuilder.durable(ORDER_QUEUE)
                // 消费失败被 nack 且不重回时，交给重试交换机（不是直接进死信队列）
                .deadLetterExchange(RETRY_EXCHANGE)
                .deadLetterRoutingKey(RETRY_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding orderBinding() {
        return BindingBuilder.bind(orderQueue()).to(orderExchange()).with(ORDER_ROUTING_KEY);
    }

    @Bean
    public DirectExchange retryExchange() {
        return ExchangeBuilder.directExchange(RETRY_EXCHANGE).durable(true).build();
    }

    @Bean
    public Queue retryQueue() {
        return QueueBuilder.durable(RETRY_QUEUE)
                // 消息在此停留 RETRY_TTL_MS，到期后自动回到业务交换机
                .ttl(RETRY_TTL_MS)
                .deadLetterExchange(ORDER_EXCHANGE)
                .deadLetterRoutingKey(ORDER_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding retryBinding() {
        return BindingBuilder.bind(retryQueue()).to(retryExchange()).with(RETRY_ROUTING_KEY);
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return ExchangeBuilder.directExchange(DEAD_LETTER_EXCHANGE).durable(true).build();
    }

    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
    }

    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue()).to(deadLetterExchange()).with(DEAD_LETTER_ROUTING_KEY);
    }

    /** 持久化探针：只声明队列，不绑定交换机、不挂消费者。 */
    @Bean
    public Queue persistProbeQueue() {
        return QueueBuilder.durable(PERSIST_PROBE_QUEUE).build();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                         MessageConverter jsonMessageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter);

        // 消息路由不到任何队列时退回给生产者（配合 spring.rabbitmq.publisher-returns: true）
        template.setMandatory(true);

        // publisher confirm：Broker 收到消息后的回执。
        // 注意它只代表 broker 收下了，不代表消息已落盘、更不代表已进队列。
        template.setConfirmCallback((correlationData, ack, cause) -> {
            String id = correlationData != null ? correlationData.getId() : "(unknown)";
            if (Boolean.TRUE.equals(ack)) {
                log.info("[confirm] Broker 已确认消息: id={}", id);
            } else {
                log.warn("[confirm] Broker 拒绝消息: id={}, cause={}", id, cause);
            }
        });

        // publisher return：消息进了交换机，但没能路由到任何队列
        template.setReturnsCallback(returned -> log.warn(
                "[return] 消息未能路由到队列: exchange={}, routingKey={}, replyText={}",
                returned.getExchange(), returned.getRoutingKey(), returned.getReplyText()));

        return template;
    }
}
