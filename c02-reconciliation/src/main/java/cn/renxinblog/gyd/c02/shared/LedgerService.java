package cn.renxinblog.gyd.c02.shared;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

/**
 * 单方账本服务 —— 封装借贷记账法两项硬约束：
 *
 * 1. **借贷平衡**：同一次调用写入的所有分录，借方合计必须等于贷方合计。
 *    校验在此处完成，不合法的调用直接抛异常，数据库侧 Postgres 普通 CHECK 无法跨行校验。
 *
 * 2. **原子提交**：所有分录在同一数据库事务中写入。
 *    Spring 的 @Transactional 保证要么全部成功，要么全部回滚。
 *
 * 外部对账校验的格式就是拉取这些分录做匹配，所以写入时必须带完整业务身份
 *（tradeId + accountId + direction + amountCents）。
 */
@Component
@Profile("bank-a | bank-b")
public class LedgerService {

    private final JournalEntryRepository repo;

    public LedgerService(JournalEntryRepository repo) {
        this.repo = repo;
    }

    /**
     * 写入一组分录，校验借贷平衡后在同一事务中提交。
     *
     * @throws UnbalancedException 借方合计不等于贷方合计
     */
    @Transactional
    public void postEntries(List<JournalEntry> entries) {
        long debitSum = entries.stream()
                .filter(e -> e.getDirection() == Direction.DEBIT)
                .mapToLong(JournalEntry::getAmountCents)
                .sum();
        long creditSum = entries.stream()
                .filter(e -> e.getDirection() == Direction.CREDIT)
                .mapToLong(JournalEntry::getAmountCents)
                .sum();

        if (debitSum != creditSum) {
            throw new UnbalancedException(debitSum, creditSum);
        }

        repo.saveAll(entries);
    }

    /**
     * 幂等记账：先检查 tradeId 是否已处理，未处理才写入。
     * 用于消费端防重复。
     *
     * @return true 表示这是新写入，false 表示已处理过（幂等跳过）
     */
    @Transactional
    public boolean postIfNew(List<JournalEntry> entries) {
        String tradeId = entries.getFirst().getTradeId();
        if (repo.existsByTradeId(tradeId)) {
            return false;
        }
        postEntries(entries);
        return true;
    }

    /**
     * 冲正：按 trade_id 找到该交易的完整 journal（所有已入账的原始分录），
     * 为每条生成等额反向分录，校验整组借贷平衡后在同一事务写入。
     *
     * 一笔账务交易（一个 trade_id）通常包含两条分录（客户存款 + 央行结算户），
     * 冲正必须对整组分录一起反向，不能只冲其中一条——否则会破坏借贷平衡。
     *
     * @param tradeId 被冲正的交易 ID（完整 journal 的聚合键）
     * @return 生成的冲正分录数；若该交易已无原始分录则返回 0
     */
    @Transactional
    public int reverseByTradeId(String tradeId) {
        List<JournalEntry> originals = repo.findOriginalByTradeId(tradeId);
        if (originals.isEmpty()) {
            return 0;
        }

        List<JournalEntry> reversals = new ArrayList<>();
        for (JournalEntry original : originals) {
            String reversalEntryId = "REV-" + original.getEntryId();
            // 已有冲正记录则幂等跳过该条
            if (repo.findByEntryId(reversalEntryId) != null) {
                continue;
            }
            Direction revDir = original.getDirection() == Direction.DEBIT
                    ? Direction.CREDIT : Direction.DEBIT;
            reversals.add(new JournalEntry(
                    reversalEntryId,
                    original.getTradeId(),
                    original.getAccountId(),
                    revDir,
                    original.getAmountCents(),
                    original.getTradeTime(),
                    original.getEntryId()
            ));
        }

        // 整组反向分录写入时同样受借贷平衡约束（复用 postEntries 的校验）
        if (!reversals.isEmpty()) {
            postEntries(reversals);
        }
        return reversals.size();
    }
}