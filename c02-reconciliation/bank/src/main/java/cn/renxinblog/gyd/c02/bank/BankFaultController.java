package cn.renxinblog.gyd.c02.bank;

import cn.renxinblog.gyd.c02.shared.domain.Bank;
import cn.renxinblog.gyd.c02.shared.domain.BookingRule;
import cn.renxinblog.gyd.c02.shared.ledger.JournalEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 故障注入端点 —— <b>仅 demo 用，生产环境不会暴露</b>。
 *
 * <p>为什么需要它：正常路径下三本账总是平的。{@code postIfNew} 的幂等检查会挡住重复消息，
 * 结算记录永远是 {@code SETTLED}，对账 Job 跑完只会输出「全部平账」。文章第五节（分级处理）、
 * 第六节（冲正）、第八节（闭环验收）讲的差异场景，一次都触发不了。
 *
 * <p>这三个端点分别对应文章第一节「三幕链条会断在哪里」那张表的三个断点：
 * <ul>
 *   <li>{@code /fault/miss} —— 入账通知丢了：结算已确认、本行漏记 → MISSING</li>
 *   <li>{@code /fault/duplicate} —— 通知重复投递且幂等失效：本行多记一组 → DUPLICATE</li>
 *   <li>{@code /fault/wrong-amount} —— 记账金额与结算记录不符 → AMOUNT_MISMATCH</li>
 * </ul>
 * 第四个断点（结算侧无记录 / 状态非 SETTLED）由结算服务的
 * {@code /api/settlement/fault/pending} 注入。
 *
 * <p>刻意与 {@link BankController} 分开：那边是对账 Job 会真实调用的业务接口，
 * 这边是实验脚手架。分开之后，读者不用在业务代码里分辨「哪些是设计、哪些是为了跑实验」。
 */
@RestController
@RequestMapping("/api/bank/fault")
public class BankFaultController {

    private static final Logger log = LoggerFactory.getLogger(BankFaultController.class);

    private final JournalEntryRepository repo;
    private final LedgerService ledger;
    private final BankProperties props;

    public BankFaultController(JournalEntryRepository repo, LedgerService ledger, BankProperties props) {
        this.repo = repo;
        this.ledger = ledger;
        this.props = props;
    }

    /**
     * 注入「漏记」：对一笔结算已成功的交易，本行不记账（模拟入账通知丢失）。
     *
     * <p>实现上就是「什么都不做」——结算侧有记录、本行账本没有，对账自然判为 MISSING。
     * 端点存在的意义是让实验脚本能显式声明「这一笔本行故意漏记」，
     * 并且调用前会先清掉本行该 tradeId 已有的分录（否则正常消费已经记上了，注入不出漏记）。
     *
     * <p>POST body: { tradeId }
     */
    @PostMapping("/miss")
    public Map<String, Object> miss(@RequestBody Map<String, String> body) {
        String tradeId = body.get("tradeId");
        Bank self = props.bank();

        // 正常路径下消费者可能已经记过账了；漏记场景要求本行账本上没有这笔的分录
        List<JournalEntry> existing = repo.findOriginalByTradeId(tradeId);
        if (!existing.isEmpty()) {
            existing.forEach(repo::delete);
            log.warn("[{}][fault] 清除本行已记的 {} 条分录，制造漏记现场: tradeId={}",
                    self.label(), existing.size(), tradeId);
        }

        log.warn("[{}][fault] 注入漏记: tradeId={} 本行账本无分录（模拟入账通知丢失）",
                self.label(), tradeId);
        return Map.of("status", "ok", "tradeId", tradeId,
                "action", "skipped-booking", "cleared", existing.size());
    }

    /**
     * 注入「重复入账」：绕过幂等，对一笔已记账的 tradeId 再写一组分录。
     *
     * <p>对应文章第七节「幂等失效」的场景：消费端没做唯一性控制，同一条清算消息被处理两次。
     * 注入的分录用 {@code DUP-} 前缀，与正常记账的 {@code E-} 前缀区分开，
     * 这样人工裁决时能看出「哪一组是多余的」——而对账 Job 自己看不出，这正是它只标记不自动冲正的原因。
     *
     * <p>POST body: { tradeId, fromBank, amountCents, settledAt }
     */
    @PostMapping("/duplicate")
    public Map<String, Object> duplicate(@RequestBody Map<String, Object> body) {
        String tradeId = (String) body.get("tradeId");
        String fromBank = (String) body.get("fromBank");
        long amountCents = ((Number) body.get("amountCents")).longValue();
        Instant settledAt = Instant.parse((String) body.get("settledAt"));

        Bank self = props.bank();
        boolean isPayer = self.code().equals(fromBank);

        List<JournalEntry> base = BookingRule.buildEntries(
                self, isPayer, tradeId, amountCents, settledAt, "DUP");

        // entryId 加纳秒后缀绕开唯一约束——重复入账恰恰要突破唯一性，
        // 否则数据库的 UNIQUE(entry_id) 会替我们把幂等做掉，注入不出这个场景
        List<JournalEntry> dup = new ArrayList<>();
        for (JournalEntry e : base) {
            dup.add(new JournalEntry(e.getEntryId() + "-" + System.nanoTime(),
                    e.getTradeId(), e.getAccountId(), e.getDirection(),
                    e.getAmountCents(), e.getTradeTime(), null));
        }

        // 直接 postEntries（不走 postIfNew），刻意绕过幂等检查
        ledger.postEntries(dup);
        log.warn("[{}][fault] 注入重复入账: tradeId={}, 多写 {} 条分录（绕过幂等）",
                self.label(), tradeId, dup.size());
        dup.forEach(e -> log.warn("[{}][fault]   {}", self.label(), e.describe()));

        return Map.of("status", "ok", "tradeId", tradeId, "injected", dup.size(),
                "entryIds", dup.stream().map(JournalEntry::getEntryId).toList());
    }

    /**
     * 注入「金额不符」：写一组金额与结算记录不一致的分录。
     *
     * <p>对应文章第五节 AMOUNT_MISMATCH 分支。注意注入的分录自身仍是借贷平衡的
     * （两条都是错误金额）——这正好印证文章第二节：金额记错，账依然是平的，
     * 试算平衡发现不了，只有跨方核对才能发现。
     *
     * <p>POST body: { tradeId, fromBank, amountCents（错误金额）, settledAt }
     */
    @PostMapping("/wrong-amount")
    public Map<String, Object> wrongAmount(@RequestBody Map<String, Object> body) {
        String tradeId = (String) body.get("tradeId");
        String fromBank = (String) body.get("fromBank");
        long wrongCents = ((Number) body.get("amountCents")).longValue();
        Instant settledAt = Instant.parse((String) body.get("settledAt"));

        Bank self = props.bank();
        boolean isPayer = self.code().equals(fromBank);

        // 先清掉正常消费记上的正确分录，否则同一 tradeId 下会同时存在正确与错误两组，
        // 对账会判成 DUPLICATE 而不是 AMOUNT_MISMATCH
        List<JournalEntry> existing = repo.findOriginalByTradeId(tradeId);
        existing.forEach(repo::delete);
        if (!existing.isEmpty()) {
            log.warn("[{}][fault] 清除本行已记的 {} 条正确分录: tradeId={}",
                    self.label(), existing.size(), tradeId);
        }

        List<JournalEntry> entries = BookingRule.buildEntries(
                self, isPayer, tradeId, wrongCents, settledAt, "WRONG");
        ledger.postEntries(entries);
        log.warn("[{}][fault] 注入金额不符: tradeId={}, 记成 ¥{}.{}（与结算记录不一致，但本行账仍是平的）",
                self.label(), tradeId, wrongCents / 100, wrongCents % 100);
        entries.forEach(e -> log.warn("[{}][fault]   {}", self.label(), e.describe()));

        return Map.of("status", "ok", "tradeId", tradeId, "wrongAmountCents", wrongCents);
    }

    /**
     * 清空本行账本 —— 实验复位用。
     *
     * <p>为什么需要：对账 Job 每轮都拉「当日全部」分录，数据是累积的。跑第二遍实验时，
     * 上一遍注入的重复入账、错误金额还在账上，简报里的差异数会一路涨（共 2 笔 → 3 笔 → 7 笔），
     * 每个场景的输出里都会混进别的场景留下的差异，读者分不清哪条是本次注入造成的。
     * 复位之后每次运行都从「零笔交易」开始，场景之间互不污染。
     *
     * <p>这是 demo 专用的破坏性操作，生产环境的账本一条都不能删——真实系统靠冲正修正，
     * 不靠删数据（详见文章第六节：原始分录永远保留）。
     *
     * <p>POST /api/bank/fault/reset
     */
    @PostMapping("/reset")
    public Map<String, Object> reset() {
        long before = repo.count();
        repo.deleteAll();
        log.warn("[{}][fault] 已清空本行账本，删除 {} 条分录（实验复位）", props.bank().label(), before);
        return Map.of("status", "ok", "bank", props.bank().label(), "deleted", before);
    }

    /**
     * 人工裁决的执行入口：冲正指定的若干条分录。
     *
     * <p>对账 Job 判出 DUPLICATE 后不会自动冲正（它回答不了「冲哪一组」），
     * 人工看完账本、确认哪组多余之后，调这个端点把那一组冲掉。
     * 这就是文章第五节「挂起等人裁决」之后那一步的落地。
     *
     * <p>POST body: { tradeId, entryIds: [...] }
     */
    @PostMapping("/adjudicate-reverse")
    public Map<String, Object> adjudicateReverse(@RequestBody Map<String, Object> body) {
        String tradeId = (String) body.get("tradeId");
        @SuppressWarnings("unchecked")
        List<String> entryIds = (List<String>) body.get("entryIds");

        int reversed = ledger.reverseByEntryIds(tradeId, entryIds);
        log.info("[{}] 人工裁决冲正完成: tradeId={}, 冲正 {} 条, entryIds={}",
                props.bank().label(), tradeId, reversed, entryIds);

        return Map.of("status", "ok", "tradeId", tradeId, "reversed", reversed);
    }
}
