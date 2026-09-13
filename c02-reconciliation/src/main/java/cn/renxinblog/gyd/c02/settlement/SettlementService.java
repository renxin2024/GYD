package cn.renxinblog.gyd.c02.settlement;

import cn.renxinblog.gyd.c02.shared.MqCommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * 结算服务：生成清算结果，通过 RabbitMQ 广播给付款行和收款行。
 *
 * 一笔跨行转账在结算侧的流程：
 * 1. 生成一条 settlement_record（状态=SETTLED）
 * 2. 发送清算消息到 RabbitMQ（付款行和收款行各自消费后记账）
 * 3. 对账 Job 以 settlement_record 为权威来源做三方核对
 */
@Service
@Profile("settlement")
public class SettlementService {

    private static final Logger log = LoggerFactory.getLogger(SettlementService.class);

    private final SettlementRepository repo;
    private final RabbitTemplate rabbitTemplate;

    public SettlementService(SettlementRepository repo, RabbitTemplate rabbitTemplate) {
        this.repo = repo;
        this.rabbitTemplate = rabbitTemplate;
    }

    /**
     * 创建一笔清算记录并广播。
     *
     * @param fromBank 付款银行代码（ICBC / CCB）
     * @param toBank   收款银行代码
     * @param amountCents 金额（分）
     * @return 生成的 tradeId
     */
    @Transactional
    public String settle(String fromBank, String toBank, long amountCents) {
        String tradeId = "TRD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        Instant now = Instant.now();

        SettlementRecord record = new SettlementRecord(tradeId, fromBank, toBank, amountCents, now);
        repo.save(record);
        log.info("[settlement] 清算完成: tradeId={}, {}→{} {}分", tradeId, fromBank, toBank, amountCents);

        // 分别投递到付款行和收款行的队列，保证两边都收到同一笔清算指令
        rabbitTemplate.convertAndSend(MqCommonConfig.SETTLEMENT_EXCHANGE,
                MqCommonConfig.BANK_A_ROUTING_KEY, record);
        rabbitTemplate.convertAndSend(MqCommonConfig.SETTLEMENT_EXCHANGE,
                MqCommonConfig.BANK_B_ROUTING_KEY, record);
        log.info("[settlement] 已广播清算消息: tradeId={} → 付款行+收款行", tradeId);

        return tradeId;
    }
}