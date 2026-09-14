package cn.renxinblog.gyd.c02.settlement;

import cn.renxinblog.gyd.c02.shared.clearing.SettlementRecord;

import cn.renxinblog.gyd.c02.shared.mq.MqCommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * 结算服务 —— 文章第一节三幕记账里的第二幕，也是 demo 里扮演「央行」的那个角色。
 *
 * <p>一笔跨行转账在结算侧的流程：
 * <ol>
 *   <li>生成一条 settlement_record（状态=SETTLED，即「已结算」）</li>
 *   <li>把清算结果分别投递到付款行和收款行的队列，两边各自消费后记账</li>
 *   <li>对账 Job 以 settlement_record 为权威来源做三方核对</li>
 * </ol>
 *
 * <p>这条记录就是对账的裁判数据：文章第一节说过，工行和建行互对吵不清谁错，
 * 但「钱到底转没转成」这个动作只发生在结算侧，所以以它为准。
 * 也正因为它权威，{@link #settle} 只产出 SETTLED；要复现「结算侧状态未定」的待查场景，
 * 走 {@link SettlementFaultController} 显式注入。
 */
@Service
public class SettlementService {

    private static final Logger log = LoggerFactory.getLogger(SettlementService.class);

    private final SettlementRepository repo;
    private final RabbitTemplate rabbitTemplate;

    public SettlementService(SettlementRepository repo, RabbitTemplate rabbitTemplate) {
        this.repo = repo;
        this.rabbitTemplate = rabbitTemplate;
    }

    /**
     * 创建一笔清算记录并广播给两家银行。
     *
     * @param fromBank    付款银行代码（ICBC 工行 / CCB 建行）
     * @param toBank      收款银行代码
     * @param amountCents 金额（分）
     * @return 生成的 tradeId
     */
    @Transactional
    public String settle(String fromBank, String toBank, long amountCents) {
        String tradeId = newTradeId();
        SettlementRecord record = saveSettled(tradeId, fromBank, toBank, amountCents);
        broadcast(record);
        return tradeId;
    }

    /** 生成清算指令 ID —— 贯穿结算记录、银行分录、对账匹配的业务身份 */
    String newTradeId() {
        return "TRD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    /** 落一条已结算记录 */
    SettlementRecord saveSettled(String tradeId, String fromBank, String toBank, long amountCents) {
        SettlementRecord record = new SettlementRecord(tradeId, fromBank, toBank, amountCents, Instant.now());
        repo.save(record);
        log.info("[settlement] 清算完成: tradeId={}, {}→{}, 金额=¥{}.{}",
                tradeId, fromBank, toBank, amountCents / 100, amountCents % 100);
        return record;
    }

    /** 分别投递到付款行和收款行的队列，保证两边都收到同一笔清算指令 */
    void broadcast(SettlementRecord record) {
        rabbitTemplate.convertAndSend(MqCommonConfig.SETTLEMENT_EXCHANGE,
                MqCommonConfig.BANK_A_ROUTING_KEY, record);
        rabbitTemplate.convertAndSend(MqCommonConfig.SETTLEMENT_EXCHANGE,
                MqCommonConfig.BANK_B_ROUTING_KEY, record);
        log.info("[settlement] 已广播清算消息: tradeId={} → 付款行 + 收款行", record.getTradeId());
    }
}
