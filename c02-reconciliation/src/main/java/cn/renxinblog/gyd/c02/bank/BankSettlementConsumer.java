package cn.renxinblog.gyd.c02.bank;

import cn.renxinblog.gyd.c02.shared.Direction;
import cn.renxinblog.gyd.c02.shared.JournalEntry;
import cn.renxinblog.gyd.c02.shared.LedgerService;
import cn.renxinblog.gyd.c02.settlement.SettlementRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 银行侧清算消息消费者。
 *
 * 监听本行专属队列（付款行监听 bank-a 队列，收款行监听 bank-b 队列），
 * 根据消息里的 fromBank/toBank 判断自己是付款方还是收款方，生成对应的借贷分录。
 *
 * 幂等处理：消费前检查 tradeId 是否已处理。如果同一条清算消息收到两次（MQ 重投），
 * 第二条被 {@link LedgerService#postIfNew} 幂等跳过，不会重复记账。
 *
 * 会计分录结构（以 bank_a 付款为例，一笔清算消息产生两条分录）：
 * <pre>
 *   DEBIT  bank_a_cust   客户存款减少（银行对客户的负债减少）
 *   CREDIT bank_a_cbdc   央行结算账户减少（银行的资产减少）
 * </pre>
 */
@Component
@Profile("bank-a | bank-b")
public class BankSettlementConsumer {

    private static final Logger log = LoggerFactory.getLogger(BankSettlementConsumer.class);

    private final LedgerService ledger;
    private final BankProperties props;

    public BankSettlementConsumer(LedgerService ledger, BankProperties props) {
        this.ledger = ledger;
        this.props = props;
    }

    /**
     * 监听本行队列。队列名来自配置 gyd.c02.bank.queue，
     * bank-a 配 gyd.c02.settlement.bank-a，bank-b 配 gyd.c02.settlement.bank-b。
     */
    @RabbitListener(queues = "${gyd.c02.bank.queue}")
    public void onSettlement(SettlementRecord settlement) {
        String tradeId = settlement.getTradeId();
        String code = props.getCode();

        // 判断当前银行在这笔交易中的角色
        boolean isPayer = code.equals(settlement.getFromBank());
        boolean isPayee = code.equals(settlement.getToBank());
        if (!isPayer && !isPayee) {
            log.debug("[{}] 非本方交易，跳过: tradeId={}", code, tradeId);
            return;
        }

        Direction custDir;
        String memo;
        if (isPayer) {
            custDir = Direction.DEBIT;
            memo = "付款";
        } else {
            custDir = Direction.CREDIT;
            memo = "收款";
        }

        String idCust = "E-" + tradeId + "-" + code + "-CUST";
        String idCbdc = "E-" + tradeId + "-" + code + "-CBDC";

        Direction cbdcDir = (custDir == Direction.DEBIT) ? Direction.CREDIT : Direction.DEBIT;
        String acctCust = code.toLowerCase() + "_cust";
        String acctCbdc = code.toLowerCase() + "_cbdc";

        JournalEntry custEntry = new JournalEntry(idCust, tradeId, acctCust,
                custDir, settlement.getAmountCents(), settlement.getSettledAt(), null);
        JournalEntry cbdcEntry = new JournalEntry(idCbdc, tradeId, acctCbdc,
                cbdcDir, settlement.getAmountCents(), settlement.getSettledAt(), null);

        boolean isNew = ledger.postIfNew(List.of(custEntry, cbdcEntry));
        if (isNew) {
            log.info("[{}] {}成功: tradeId={}, 金额={}分, cust={}, cbdc={}",
                    code, memo, tradeId, settlement.getAmountCents(),
                    acctCust + " " + custDir, acctCbdc + " " + cbdcDir);
        } else {
            log.info("[{}] 幂等跳过重复消息: tradeId={}", code, tradeId);
        }
    }
}