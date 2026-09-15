package cn.renxinblog.gyd.c03.bank;

import cn.renxinblog.gyd.c03.shared.domain.Bank;
import cn.renxinblog.gyd.c03.shared.domain.BookingRule;
import cn.renxinblog.gyd.c03.shared.ledger.JournalEntry;

import cn.renxinblog.gyd.c03.shared.clearing.SettlementRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 银行侧清算消息消费者 —— 文章第一节三幕记账里的第一幕（工行）和第三幕（建行）。
 *
 * <p>监听本行专属队列（付款行监听 bank-a 队列，收款行监听 bank-b 队列），
 * 收到结算服务广播的清算结果后，按 {@link BookingRule} 在本行账本上写入一组
 * （两条）借贷平衡的分录。
 *
 * <p>幂等处理：消费前检查 tradeId 是否已处理。如果同一条清算消息收到两次（MQ 重投），
 * 第二条被 {@link LedgerService#postIfNew} 幂等跳过，不会重复记账。这正是文章第七节
 * 「为什么对账不能替代幂等」里的事前防护——它挡住了第三个断点（通知重复投递）里的大部分情况。
 *
 * <p>本行是付款方还是收款方，看结算记录里的 fromBank / toBank：本行代码等于 fromBank
 * 就是付款方（客户存款减少），等于 toBank 就是收款方（客户存款增加）。
 */
@Component
public class BankSettlementConsumer {

    private static final Logger log = LoggerFactory.getLogger(BankSettlementConsumer.class);

    private final LedgerService ledger;
    private final BankProperties props;

    public BankSettlementConsumer(LedgerService ledger, BankProperties props) {
        this.ledger = ledger;
        this.props = props;
    }

    /**
     * 监听本行队列。队列名来自配置 gyd.c03.bank.queue，
     * bank-a 配 gyd.c03.settlement.bank-a，bank-b 配 gyd.c03.settlement.bank-b。
     */
    @RabbitListener(queues = "${gyd.c03.bank.queue}")
    public void onSettlement(SettlementRecord settlement) {
        String tradeId = settlement.getTradeId();
        Bank self = props.bank();

        // 判断本行在这笔交易里的角色：付款方 or 收款方
        boolean isPayer = self.code().equals(settlement.getFromBank());
        boolean isPayee = self.code().equals(settlement.getToBank());
        if (!isPayer && !isPayee) {
            log.debug("[{}] 非本方交易，跳过: tradeId={}", self.label(), tradeId);
            return;
        }

        List<JournalEntry> entries = BookingRule.buildEntries(
                self, isPayer, tradeId, settlement.getAmountCents(), settlement.getSettledAt(), "E");

        boolean isNew = ledger.postIfNew(entries);
        if (isNew) {
            log.info("[{}] {}记账成功: tradeId={}, 金额=¥{}.{}",
                    self.label(), isPayer ? "付款" : "收款", tradeId,
                    settlement.getAmountCents() / 100, settlement.getAmountCents() % 100);
            entries.forEach(e -> log.info("[{}]   {}", self.label(), e.describe()));
        } else {
            log.info("[{}] 幂等跳过重复消息: tradeId={}（该清算指令已记过账）", self.label(), tradeId);
        }
    }
}
