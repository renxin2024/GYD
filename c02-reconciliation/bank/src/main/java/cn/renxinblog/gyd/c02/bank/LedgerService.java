package cn.renxinblog.gyd.c02.bank;

import cn.renxinblog.gyd.c02.shared.domain.Direction;
import cn.renxinblog.gyd.c02.shared.ledger.JournalEntry;
import cn.renxinblog.gyd.c02.shared.domain.UnbalancedException;

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
     * 一笔账务交易（一个 trade_id）通常包含两条分录（客户存款 + 存放央行款项），
     * 冲正必须对整组分录一起反向，不能只冲其中一条——否则会破坏借贷平衡。
     *
     * <p>适用场景：这笔交易整笔都要撤销。注意它会把该 tradeId 下<b>所有</b>原始分录都冲掉，
     * 所以<b>不能</b>用于「重复入账只冲多余那组」——那种情况一个 tradeId 下有两组分录
     * （合法的一组 + 多余的一组），整组冲正会把合法的也冲掉，得用
     * {@link #reverseByEntryIds(String, List)} 由人工指定冲哪几条。
     *
     * @param tradeId 被冲正的交易 ID（完整 journal 的聚合键）
     * @return 生成的冲正分录数；若该交易已无原始分录则返回 0
     */
    @Transactional
    public int reverseByTradeId(String tradeId) {
        return reverseEntries(repo.findOriginalByTradeId(tradeId));
    }

    /**
     * 冲正指定的若干条分录 —— 人工裁决的执行入口。
     *
     * <p>文章第五节的核心论点：重复入账时，{@code trade_id} 相同只能说明「多记了」，
     * 却判断不出「哪一组是正确的那组、哪一组该冲正」，所以对账 Job 只标记差异、不自动冲正。
     * 但人工裁决给出「冲这几条」之后，系统必须能精确执行——这就是本方法的职责。
     *
     * <p>传入的分录必须自身借贷平衡（通常是完整的一组：客户存款 + 存放央行款项），
     * 否则 {@link #postEntries} 的平衡校验会拒绝。
     *
     * @param tradeId  交易 ID（用于定位与日志）
     * @param entryIds 要冲正的原始分录业务流水号
     * @return 生成的冲正分录数
     */
    @Transactional
    public int reverseByEntryIds(String tradeId, List<String> entryIds) {
        List<JournalEntry> originals = repo.findOriginalByTradeId(tradeId).stream()
                .filter(e -> entryIds.contains(e.getEntryId()))
                .toList();
        if (originals.size() != entryIds.size()) {
            throw new IllegalArgumentException(String.format(
                    "tradeId=%s 下找不到全部待冲正分录: 期望 %d 条 %s，实际匹配 %d 条",
                    tradeId, entryIds.size(), entryIds, originals.size()));
        }
        return reverseEntries(originals);
    }

    /** 为一组原始分录生成等额反向分录并写入（冲正的实际动作，两个入口共用） */
    private int reverseEntries(List<JournalEntry> originals) {
        if (originals.isEmpty()) {
            return 0;
        }

        List<JournalEntry> reversals = new ArrayList<>();
        for (JournalEntry original : originals) {
            String reversalEntryId = "REV-" + original.getEntryId();
            // 已有冲正记录则幂等跳过该条：重复执行冲正不会写出第二组反向分录
            if (repo.findByEntryId(reversalEntryId) != null) {
                continue;
            }
            Direction revDir = original.getDirection().opposite();
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