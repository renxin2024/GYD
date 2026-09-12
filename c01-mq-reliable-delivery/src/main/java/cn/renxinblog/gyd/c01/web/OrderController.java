package cn.renxinblog.gyd.c01.web;

import cn.renxinblog.gyd.c01.message.OrderMessage;
import cn.renxinblog.gyd.c01.producer.OrderProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

/**
 * 手工验证整条链路的入口，每个端点对应正文里的一个判断。
 */
@RestController
@RequestMapping("/gyd/c01/order")
public class OrderController {

    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    /**
     * 下一单并投递消息（同步等 Broker 回执）。
     *
     * orderId 里带 "fail" 可以演示「消费失败 → 延迟重试 → 死信队列」这条链路；
     * 带 "slow" 则每次处理慢一拍（耗时由 {@code gyd.c01.slow-processing-ms} 控制）。
     */
    @PostMapping
    public Map<String, Object> create(@RequestParam String orderId,
                                      @RequestParam(defaultValue = "100") long amount) throws Exception {
        OrderMessage message = new OrderMessage(orderId, "u-0001", amount, Instant.now().toString());
        orderProducer.sendAndWaitConfirm(message);
        return Map.of("sent", true, "orderId", orderId);
    }

    /**
     * 批量投递，用来灌出稳定的积压，观察 prefetch 与未确认消息的关系。
     *
     * 配合 orderId 前缀 "slow"，让消费端处理得足够慢，方便采样。
     */
    @PostMapping("/burst")
    public Map<String, Object> burst(@RequestParam(defaultValue = "slow") String prefix,
                                     @RequestParam(defaultValue = "50") int count) {
        orderProducer.sendBurst(prefix, count);
        return Map.of("sent", count, "prefix", prefix);
    }

    /**
     * 发一条路由不到队列的消息。
     *
     * 用来对照观察：消息进了交换机、confirm 仍然返回 ack，
     * 但没有任何队列收到它——没有 return 回调就等于静默丢消息。
     */
    @PostMapping("/unroutable")
    public Map<String, Object> unroutable(@RequestParam String orderId) {
        OrderMessage message = new OrderMessage(orderId, "u-0001", 0L, Instant.now().toString());
        orderProducer.sendUnroutable(message);
        return Map.of("sent", true, "orderId", orderId,
                "note", "routingKey=order.nowhere 无队列绑定，观察 return 回调");
    }

    /**
     * 投递一条持久化探针消息到没有消费者的队列，用于「重启 broker 看还在不在」的实验。
     *
     * persistent=true 与 false 各发一条，重启后对比数量即可看出
     * confirm 的 ack 与「消息真的落盘」是两件事。
     */
    @PostMapping("/probe")
    public Map<String, Object> probe(@RequestParam String id,
                                     @RequestParam(defaultValue = "true") boolean persistent) {
        orderProducer.sendPersistenceProbe(id, persistent);
        return Map.of("sent", true, "id", id, "persistent", persistent,
                "note", "投递到无消费者队列 gyd.c01.persist.probe，重启 broker 后对比消息数");
    }
}
