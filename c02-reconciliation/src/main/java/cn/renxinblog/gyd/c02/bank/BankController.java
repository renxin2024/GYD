package cn.renxinblog.gyd.c02.bank;

import cn.renxinblog.gyd.c02.shared.Direction;
import cn.renxinblog.gyd.c02.shared.JournalEntry;
import cn.renxinblog.gyd.c02.shared.JournalEntryRepository;
import cn.renxinblog.gyd.c02.shared.LedgerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * 银行侧 API：供对账 Job 远程拉取当日记账数据、补记漏记分录、冲正多余分录。
 */
@RestController
@Profile("bank-a | bank-b")
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
     * 补记漏记的分录（对账 Job 调用）。
     * POST body: { tradeId, fromBank, toBank, amountCents, settledAt }
     */
    @PostMapping("/compensate")
    public Map<String, String> compensate(@RequestBody Map<String, Object> body) {
        String tradeId = (String) body.get("tradeId");
        String fromBank = (String) body.get("fromBank");
        String toBank = (String) body.get("toBank");
        long amountCents = ((Number) body.get("amountCents")).longValue();
        Instant settledAt = Instant.parse((String) body.get("settledAt"));

        String code = props.getCode();
        boolean isPayer = code.equals(fromBank);
        Direction custDir = isPayer ? Direction.DEBIT : Direction.CREDIT;
        Direction cbdcDir = isPayer ? Direction.CREDIT : Direction.DEBIT;

        JournalEntry custEntry = new JournalEntry(
                "COMP-" + tradeId + "-" + code + "-CUST",
                tradeId, code.toLowerCase() + "_cust", custDir, amountCents, settledAt, null);
        JournalEntry cbdcEntry = new JournalEntry(
                "COMP-" + tradeId + "-" + code + "-CBDC",
                tradeId, code.toLowerCase() + "_cbdc", cbdcDir, amountCents, settledAt, null);

        boolean isNew = ledger.postIfNew(List.of(custEntry, cbdcEntry));
        log.info("[{}] 补记{}: tradeId={}, 金额={}分, 新写入={}",
                code, isNew ? "成功" : "跳过（已存在）", tradeId, amountCents, isNew);

        return Map.of("status", "ok", "tradeId", tradeId, "isNew", String.valueOf(isNew));
    }

    /**
     * 冲正指定交易的完整 journal（对账 Job 调用）。
     * POST body: { tradeId }
     *
     * 冲正按 trade_id 定位该交易的整组分录（客户存款 + 央行结算户），
     * 一起生成等额反向分录，保证借贷平衡。
     */
    @PostMapping("/reverse")
    public Map<String, String> reverse(@RequestBody Map<String, String> body) {
        String tradeId = body.get("tradeId");

        try {
            int reversed = ledger.reverseByTradeId(tradeId);
            log.info("[{}] 冲正完成: tradeId={}, 反向分录数={}", props.getCode(), tradeId, reversed);
            return Map.of("status", "ok", "tradeId", tradeId, "reversed", String.valueOf(reversed));
        } catch (Exception e) {
            log.warn("[{}] 冲正失败: tradeId={}, reason={}", props.getCode(), tradeId, e.getMessage());
            return Map.of("status", "skipped", "reason", e.getMessage());
        }
    }
}