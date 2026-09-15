package cn.renxinblog.gyd.c03.settlement;

import cn.renxinblog.gyd.c03.shared.clearing.SettlementRecord;
import cn.renxinblog.gyd.c03.shared.clearing.SettlementStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 结算侧故障注入 —— <b>仅 demo 用</b>。
 *
 * <p>银行侧的三个注入端点（{@code BankFaultController}）造的都是「银行记错了」，
 * 还差一类：结算侧本身状态未定。对应文章第五节决策树的最上面两层——
 * 结算侧没有对应记录，或状态不是 {@code SETTLED}（已结算），对账 Job 都不做自动修正，
 * 只标记待查、等下一批次重新核对。
 *
 * <p>注入方式是把一笔已结算的记录改回 {@code PENDING}（结算中）。
 * 对账 Job 的 {@code findSettledBetween} 只拉 {@code SETTLED} 的记录，
 * 所以这笔在结算侧「查无此记录」，而两家银行的分录都在 → 判为 NO_SETTLEMENT（待查）。
 */
@RestController
@RequestMapping("/api/settlement/fault")
public class SettlementFaultController {

    private static final Logger log = LoggerFactory.getLogger(SettlementFaultController.class);

    private final SettlementRepository repo;

    public SettlementFaultController(SettlementRepository repo) {
        this.repo = repo;
    }

    /**
     * 把一笔已结算的记录改回 PENDING（结算中），制造「结算结果未知」的待查差异。
     * POST body: { tradeId }
     */
    @PostMapping("/pending")
    public Map<String, Object> pending(@RequestBody Map<String, String> body) {
        String tradeId = body.get("tradeId");
        SettlementRecord record = repo.findByTradeId(tradeId);
        if (record == null) {
            return Map.of("status", "not-found", "tradeId", tradeId);
        }

        record.setStatus(SettlementStatus.PENDING);
        repo.save(record);
        log.warn("[settlement][fault] 注入待查: tradeId={} 状态改为 PENDING（结算中），"
                + "对账拉不到已结算记录 → 两家银行的分录都会被判为待查", tradeId);

        return Map.of("status", "ok", "tradeId", tradeId,
                "newStatus", SettlementStatus.PENDING.name());
    }

    /**
     * 清空结算记录 —— 实验复位用，与银行侧的 {@code /api/bank/fault/reset} 配套。
     *
     * <p>三本账必须一起清：只清银行侧，结算记录还在，对账会把「结算有、银行无」全判成漏记
     * 并自动补记；只清结算侧，则全部变成「银行有、结算无」的待查。实验脚本开头调这三个端点，
     * 每轮运行都从零开始。
     *
     * <p>demo 专用，生产环境的结算记录是清算权威源，不可删除。
     *
     * <p>POST /api/settlement/fault/reset
     */
    @PostMapping("/reset")
    public Map<String, Object> reset() {
        long before = repo.count();
        repo.deleteAll();
        log.warn("[settlement][fault] 已清空结算记录，删除 {} 条（实验复位）", before);
        return Map.of("status", "ok", "deleted", before);
    }

    /**
     * 生成一笔「结算侧有记录、但不广播给银行」的交易 —— 两家银行都漏记。
     *
     * <p>和 {@code BankFaultController.miss}（单边漏记）互补：那个造「一边有一边没有」，
     * 这个造「结算成功了但两边都没收到通知」。对账会在两侧各报一条 MISSING，
     * 然后按结算记录给两边都补记。
     */
    @PostMapping("/no-broadcast")
    public Map<String, Object> noBroadcast(@RequestBody Map<String, Object> body) {
        String fromBank = (String) body.getOrDefault("fromBank", "ICBC");
        String toBank = (String) body.getOrDefault("toBank", "CCB");
        long amountCents = ((Number) body.getOrDefault("amountCents", 10000L)).longValue();

        SettlementRecord record = new SettlementRecord(
                "TRD-NOBC-" + java.util.UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                fromBank, toBank, amountCents, java.time.Instant.now());
        repo.save(record);
        log.warn("[settlement][fault] 注入双边漏记: tradeId={} 结算已记录但未广播清算消息，"
                + "两家银行都不会记账", record.getTradeId());

        return Map.of("status", "ok", "tradeId", record.getTradeId(),
                "amountCents", amountCents, "broadcast", false);
    }
}
