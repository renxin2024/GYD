package cn.renxinblog.gyd.c02.shared.mq;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.DefaultJacksonJavaTypeMapper;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ 拓扑与消息转换器的共享配置。
 *
 * 这里有两个职责，都放在 shared 这个无 profile 的公共模块里，三个服务
 * （结算、付款行、收款行）通过 Gradle 依赖各自加载同一份：
 *
 * 1. 拓扑声明（exchange / 两个银行队列 / 两条绑定）。队列和交换机是 durable 的，
 *    声明是幂等的——多个服务重复声明同一份拓扑不会冲突。这一步之所以不能只放在
 *    结算服务里，是因为银行服务是「消费者」，它的 {@code @RabbitListener} 启动时
 *    就会去监听队列；如果队列没人声明，监听一个不存在的队列会直接 fatal 退出。
 *    所以拓扑声明要和消费者解耦，放在所有服务都加载的共享配置里。
 *
 * 2. 消息转换器。结算服务（发送方）和银行服务（消费方）都要用 Jackson 序列化 /
 *    反序列化 {@link cn.renxinblog.gyd.c02.shared.clearing.SettlementRecord}，
 *    不能只在某一个服务里装配，否则另一端会退回默认的 SimpleMessageConverter
 *    （Java 序列化），导致反序列化失败。
 *
 * 关键设计：两个银行各有一个独立队列，避免竞争消费。一笔跨行转账的清算结果必须
 * 同时到达付款行和收款行，所以结算服务用两条 routing key（settlement.bank-a /
 * settlement.bank-b）分别投递。如果只用一个队列、两家银行都去消费，Spring AMQP
 * 的 simple listener 会让多个消费者竞争同一队列——一条消息只被一个消费者拿到，
 * 另一家银行永远收不到，无法保证「双方都记了账」。
 */
@Configuration
public class MqCommonConfig {

    public static final String SETTLEMENT_EXCHANGE = "gyd.c02.settlement.exchange";

    /** 付款行队列 */
    public static final String BANK_A_QUEUE = "gyd.c02.settlement.bank-a";
    public static final String BANK_A_ROUTING_KEY = "settlement.bank-a";

    /** 收款行队列 */
    public static final String BANK_B_QUEUE = "gyd.c02.settlement.bank-b";
    public static final String BANK_B_ROUTING_KEY = "settlement.bank-b";

    @Bean
    public MessageConverter jsonMessageConverter() {
        JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter();
        DefaultJacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper();
        // 信任 c02 下所有会经 MQ 传输的类：消息体是 shared.clearing 里的 SettlementRecord，
        // 但生产方（settlement）和消费方（bank）各自的领域类也可能被序列化，
        // 一并信任。包前缀匹配会自动覆盖子包（如 settlement.recon、shared.ledger）。
        typeMapper.setTrustedPackages(
                "cn.renxinblog.gyd.c02.shared",
                "cn.renxinblog.gyd.c02.bank",
                "cn.renxinblog.gyd.c02.settlement"
        );
        converter.setJavaTypeMapper(typeMapper);
        return converter;
    }

    @Bean
    public DirectExchange settlementExchange() {
        return ExchangeBuilder.directExchange(SETTLEMENT_EXCHANGE).durable(true).build();
    }

    /** 付款行队列 */
    @Bean
    public Queue bankAQueue() {
        return QueueBuilder.durable(BANK_A_QUEUE).build();
    }

    /** 收款行队列 */
    @Bean
    public Queue bankBQueue() {
        return QueueBuilder.durable(BANK_B_QUEUE).build();
    }

    @Bean
    public Binding bankABinding() {
        return BindingBuilder.bind(bankAQueue())
                .to(settlementExchange()).with(BANK_A_ROUTING_KEY);
    }

    @Bean
    public Binding bankBBinding() {
        return BindingBuilder.bind(bankBQueue())
                .to(settlementExchange()).with(BANK_B_ROUTING_KEY);
    }
}
