package cn.renxinblog.gyd.c02.bank;

import cn.renxinblog.gyd.c02.shared.domain.Bank;
import cn.renxinblog.gyd.c02.shared.domain.BookingRule;
import cn.renxinblog.gyd.c02.shared.ledger.JournalEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * 银行侧 API：供对账 Job 远程拉取当日记账数据、补记漏记分录、冲正多余分录。
 *
 * <p>另有一组 {@code /api/bank/fault/**} 故障注入端点，<b>仅供 demo 复现文章讲的差异场景</b>，
 * 生产环境不会暴露。没有这些端点，正常路径下三本账总是平的，文章第五节（分级处理）、
 * 第六节（冲正）、第八节（闭环验收）一次都触发不了。
 */
@RestController
@RequestMapping("/api/bank")
public class BankController {

    private static final Logger log = LoggerFactory.getLogger(BankController.class);

    private final JournalEntryRepository repo;
    private final LedgerService ledger;
    private final BankProperties props;

    public BankController(JournalEntryRepository repo, LedgerService ledger, BankProperties props) {
        this.repo = repo;
        this.ledger = ledger;
        this.props = props;
    }

    /** 按记账时间范围拉取净分录（已被冲正的 journal 不再参与对账） */
    @GetMapping("/entries")
    public List<JournalEntry> getEntries(
            @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant start,
            @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant end) {
        return repo.findNetByBookedAtBetween(start, end);
    }

    /**
     * 拉取某笔交易的完整账本轨迹：原始分录 + 冲正分录，按写入顺序排列。
     *
     * <p>这就是文章第六节「原始分录和冲正分录并排看」那张表的数据来源——
     * 冲正是追加式的，原始分录永远保留，所以这里能同时看到「发生过什么」和「怎么修正的」。
     */
    @GetMapping("/journal/{tradeId}")
    public List<JournalEntry> getJournal(@PathVariable String tradeId) {
        return repo.findAllByTradeId(tradeId);
    }

    /**
     * 补记漏记的分录（对账 Job 在 MISSING 分支调用）。
     * POST body: { tradeId, fromBank, toBank, amountCents, settledAt }
     *
     * <p>补记复用与正常消费相同的 {@link BookingRule}，所以补的也是「一组」分录
     * （客户存款 + 存放央行款项），同样受借贷平衡和幂等约束——不是孤零零补一条。
     */
    @PostMapping("/compensate")
    public Map<String, String> compensate(@RequestBody Map<String, Object> body) {
        String tradeId = (String) body.get("tradeId");
        String fromBank = (String) body.get("fromBank");
        long amountCents = ((Number) body.get("amountCents")).longValue();
        Instant settledAt = Instant.parse((String) body.get("settledAt"));

        Bank self = props.bank();
        boolean isPayer = self.code().equals(fromBank);

        List<JournalEntry> entries = BookingRule.buildEntries(
                self, isPayer, tradeId, amountCents, settledAt, "COMP");

        boolean isNew = ledger.postIfNew(entries);
        log.info("[{}] 补记{}: tradeId={}, 金额={}分", self.label(),
                isNew ? "成功" : "跳过（已存在）", tradeId, amountCents);
        if (isNew) {
            entries.forEach(e -> log.info("[{}]   {}", self.label(), e.describe()));
        }

        return Map.of("status", "ok", "tradeId", tradeId, "isNew", String.valueOf(isNew));
    }

    /**
     * 冲正指定交易的完整 journal（对账 Job 在 DUPLICATE 分支人工裁决后调用，或实验脚本直接调用）。
     * POST body: { tradeId }
     *
     * <p>冲正按 trade_id 定位该交易的整组分录（客户存款 + 存放央行款项），
     * 一起生成等额反向分录，保证借贷平衡。详见文章第六节。
     */
    @PostMapping("/reverse")
    public Map<String, String> reverse(@RequestBody Map<String, String> body) {
        String tradeId = body.get("tradeId");

        try {
            int reversed = ledger.reverseByTradeId(tradeId);
            log.info("[{}] 冲正完成: tradeId={}, 反向分录数={}", props.bank().label(), tradeId, reversed);
            return Map.of("status", "ok", "tradeId", tradeId, "reversed", String.valueOf(reversed));
        } catch (Exception e) {
            log.warn("[{}] 冲正失败: tradeId={}, reason={}", props.bank().label(), tradeId, e.getMessage());
            return Map.of("status", "skipped", "reason", e.getMessage());
        }
    }
}
