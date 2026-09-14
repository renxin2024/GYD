package cn.renxinblog.gyd.c02.shared.domain;

import cn.renxinblog.gyd.c02.shared.ledger.JournalEntry;

import java.time.Instant;
import java.util.List;

/**
 * 记账规则 —— 一笔跨行转账，在「本行」账本上该记哪几条分录。
 *
 * <p>文章第一节三幕记账里，工行（付款行）和建行（收款行）各写一组两条分录，
 * 方向正好相反。这段代码把那两组分录的构造集中在一处，消费者
 * （{@code BankSettlementConsumer}）和补记端点（{@code BankController.compensate}）
 * 都复用它，避免两处各写一遍、方向判断各写一套。
 *
 * <p>以工行付款一笔 10,000 元为例，构造出的两条分录正是文章第二节那张表：
 * <pre>
 *   借  客户存款        ¥10,000   （负债减少）
 *   贷  存放央行款项     ¥10,000   （资产减少）
 * </pre>
 * 建行收款则两条方向都反过来：
 * <pre>
 *   借  存放央行款项     ¥10,000   （资产增加）
 *   贷  客户存款        ¥10,000   （负债增加）
 * </pre>
 *
 * <p>方向不写死，全部由 {@link Account#directionFor(Movement)} 按「科目性质 + 增减」推导，
 * 所以规则表改了，这里不用动。
 */
public final class BookingRule {

    private BookingRule() {
    }

    /**
     * 构造本行这一组（两条）分录。
     *
     * @param bank         本行
     * @param isPayer      本行在这笔交易里是付款方还是收款方
     * @param tradeId      清算指令 ID（两条分录共享，对账靠它匹配）
     * @param amountCents  金额（分）
     * @param tradeTime    业务发生时间（来自结算记录的 settledAt）
     * @param entryIdPrefix 分录业务流水号前缀：正常记账用 {@code E}，补记用 {@code COMP}，
     *                      便于在账本里一眼区分「原始入账」和「对账补记」
     * @return 一组借贷平衡的分录（客户存款 + 存放央行款项）
     */
    public static List<JournalEntry> buildEntries(Bank bank, boolean isPayer, String tradeId,
                                                  long amountCents, Instant tradeTime,
                                                  String entryIdPrefix) {
        // 付款行：客户存款减少（客户把钱付出去了）、存放央行减少（要付给收款行）
        // 收款行：客户存款增加（客户收到钱了）、存放央行增加（从付款行收到了）
        Movement customerMovement = isPayer ? Movement.DECREASE : Movement.INCREASE;
        Movement pbocMovement = isPayer ? Movement.DECREASE : Movement.INCREASE;

        JournalEntry customerEntry = entry(bank, Account.CUSTOMER_DEPOSIT, customerMovement,
                tradeId, amountCents, tradeTime, entryIdPrefix, "CUST");
        JournalEntry pbocEntry = entry(bank, Account.PBOC_SETTLEMENT, pbocMovement,
                tradeId, amountCents, tradeTime, entryIdPrefix, "PBOC");

        // 一减一增、金额相等 → 借贷必然平衡（由 LedgerService.postEntries 再校验一次）
        return List.of(customerEntry, pbocEntry);
    }

    private static JournalEntry entry(Bank bank, Account account, Movement movement,
                                      String tradeId, long amountCents, Instant tradeTime,
                                      String prefix, String suffix) {
        String entryId = prefix + "-" + tradeId + "-" + bank.code() + "-" + suffix;
        Direction direction = account.directionFor(movement);
        return new JournalEntry(entryId, tradeId, account.code(),
                direction, amountCents, tradeTime, null);
    }
}
