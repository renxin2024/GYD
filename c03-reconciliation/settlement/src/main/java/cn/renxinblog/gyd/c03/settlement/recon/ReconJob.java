package cn.renxinblog.gyd.c03.settlement.recon;

import cn.renxinblog.gyd.c03.shared.domain.Account;
import cn.renxinblog.gyd.c03.shared.domain.Bank;
import cn.renxinblog.gyd.c03.shared.domain.Direction;
import cn.renxinblog.gyd.c03.shared.ledger.JournalEntry;
import cn.renxinblog.gyd.c03.shared.domain.Movement;
import cn.renxinblog.gyd.c03.shared.clearing.SettlementRecord;
import cn.renxinblog.gyd.c03.settlement.SettlementRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 日终对账 Job —— 运行在结算服务进程内。
 *
 * <p>核心流程：
 * <ol>
 *   <li>通过 REST API 拉取付款行和收款行的当日记账数据</li>
 *   <li>从本地 DB 拉取结算记录（权威数据源）</li>
 *   <li>以 tradeId 为键建立三方索引，逐笔匹配</li>
 *   <li>对每条差异做分级决策：自动补记 / 挂起等人工裁决 / 等待下一批次</li>
 *   <li>闭环验收：对「已自动处理」的差异重新拉数据确认差异清零</li>
 * </ol>
 *
 * <p>匹配规则（每个科目各查一次，不是只看一个方向）：
 * <ul>
 *   <li>结算记录必须存在且状态=SETTLED（已结算）</li>
 *   <li>付款行的客户存款应有借方分录、存放央行款项应有贷方分录；收款行方向相反</li>
 *   <li>金额以结算记录为准（权威数据源）</li>
 *   <li>只比「净分录」——已被冲正的原始分录不再参与匹配</li>
 *   <li>金额不作为唯一匹配依据，tradeId 才是业务身份</li>
 * </ul>
 *
 * <p>分级处理对应文章第五节的决策树，三类结局不同：
 * <ul>
 *   <li><b>自动处理</b>：漏记 + 结算已确认 → 按结算记录补记（正确动作由权威源唯一确定）</li>
 *   <li><b>挂起等人工</b>：重复入账、金额不符 → 只告警并给出候选分录，不自动动账
 *       （对账回答不了「冲哪一组」「哪边金额是对的」）</li>
 *   <li><b>等待下一批次</b>：在途、结算侧无记录 → 不做任何修正</li>
 * </ul>
 */
@Component
public class ReconJob {

    private static final Logger log = LoggerFactory.getLogger(ReconJob.class);

    private final SettlementRepository settlementRepo;
    private final ReconAlert alert;
    private final RestTemplate restTemplate;

    private final String bankAUrl;
    private final String bankBUrl;

    public ReconJob(SettlementRepository settlementRepo,
                    ReconAlert alert,
                    @Value("${gyd.c03.recon.bank-a-url:http://localhost:18081}") String bankAUrl,
                    @Value("${gyd.c03.recon.bank-b-url:http://localhost:18082}") String bankBUrl) {
        this.settlementRepo = settlementRepo;
        this.alert = alert;
        this.bankAUrl = bankAUrl;
        this.bankBUrl = bankBUrl;
        this.restTemplate = new RestTemplate();
    }

    /** 定时执行 */
    @Scheduled(fixedDelayString = "${gyd.c03.recon.schedule-ms:60000}",
            initialDelayString = "${gyd.c03.recon.initial-delay-ms:30000}")
    public void run() {
        LocalDate today = LocalDate.now();
        Instant start = today.atStartOfDay(ZoneOffset.UTC).toInstant();
        Instant end = today.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant();

        log.info("[recon] 开始日终对账: {} → {}", start, end);

        // 1. 拉取三方数据
        List<SettlementRecord> settlements = settlementRepo.findSettledBetween(start, end);
        List<JournalEntry> bankAEntries = fetchBankEntries(bankAUrl, start, end);
        List<JournalEntry> bankBEntries = fetchBankEntries(bankBUrl, start, end);

        log.info("[recon] 拉取完成: 结算 {} 条, 工行分录 {} 条, 建行分录 {} 条",
                settlements.size(), bankAEntries.size(), bankBEntries.size());

        // 2. 以 tradeId 为键建立三方索引
        Map<String, List<JournalEntry>> aByTrade = groupByTrade(bankAEntries);
        Map<String, List<JournalEntry>> bByTrade = groupByTrade(bankBEntries);
        Map<String, SettlementRecord> settlementMap = settlements.stream()
                .collect(Collectors.toMap(SettlementRecord::getTradeId, s -> s));

        Set<String> allTradeIds = new HashSet<>(settlementMap.keySet());
        allTradeIds.addAll(aByTrade.keySet());
        allTradeIds.addAll(bByTrade.keySet());

        // 3. 逐笔匹配
        List<ReconDiff> diffs = new ArrayList<>();
        int matched = 0;
        for (String tradeId : allTradeIds) {
            SettlementRecord settlement = settlementMap.get(tradeId);
            List<JournalEntry> aEntries = aByTrade.getOrDefault(tradeId, List.of());
            List<JournalEntry> bEntries = bByTrade.getOrDefault(tradeId, List.of());

            if (settlement == null) {
                // 银行有分录、结算侧查不到已结算记录 → 待查（不自动修正）
                if (!aEntries.isEmpty()) {
                    diffs.add(new ReconDiff(tradeId, ReconDiffType.NO_SETTLEMENT, Bank.ICBC.code(),
                            entryIds(aEntries), -1, 0));
                }
                if (!bEntries.isEmpty()) {
                    diffs.add(new ReconDiff(tradeId, ReconDiffType.NO_SETTLEMENT, Bank.CCB.code(),
                            entryIds(bEntries), -1, 0));
                }
                continue;
            }

            boolean aOk = checkBank(aEntries, settlement, Bank.ICBC, tradeId, diffs);
            boolean bOk = checkBank(bEntries, settlement, Bank.CCB, tradeId, diffs);
            if (aOk && bOk) {
                matched++;
            }
        }

        alert.brief(allTradeIds.size(), matched, diffs.size());

        // 4. 分级处理差异
        for (ReconDiff diff : diffs) {
            processDiff(diff, settlementMap);
        }

        // 5. 闭环验收
        verifyClosedLoop(diffs, settlementMap, start, end);
    }

    /**
     * 核对一家银行的账。
     *
     * <p>按科目逐个查，而不是只看一个方向：本行这笔交易应该有且仅有
     * 「客户存款」和「存放央行款项」各一条净分录，方向和金额都由本行在这笔交易里的
     * 角色（付款方 / 收款方）决定。角色从结算记录的 fromBank / toBank 推导，
     * 不写死「工行一定是借方」——反向交易（建行付给工行）时方向会整体反过来。
     */
    private boolean checkBank(List<JournalEntry> entries, SettlementRecord settlement,
                              Bank bank, String tradeId, List<ReconDiff> diffs) {
        boolean isPayer = bank.code().equals(settlement.getFromBank());
        boolean isPayee = bank.code().equals(settlement.getToBank());
        if (!isPayer && !isPayee) {
            // 这家银行跟这笔交易无关（demo 里只有两家，正常不会走到）
            return true;
        }

        long expected = settlement.getAmountCents();
        Movement movement = isPayer ? Movement.DECREASE : Movement.INCREASE;
        boolean ok = true;

        for (Account account : Account.values()) {
            Direction expectedDir = account.directionFor(movement);
            List<JournalEntry> onAccount = entries.stream()
                    .filter(e -> account.code().equals(e.getAccountId()))
                    .toList();

            if (onAccount.isEmpty()) {
                diffs.add(new ReconDiff(tradeId, ReconDiffType.MISSING, bank.code(),
                        account, List.of(), expected, 0));
                ok = false;
                continue;
            }

            if (onAccount.size() > 1) {
                // 银行侧金额报「该科目净分录之和」，不是 0：重复入账时读者需要
                // 一眼看出偏差是几倍（结算 ¥10000 / 银行 ¥20000 = 多记了一组）
                long netAmount = onAccount.stream().mapToLong(JournalEntry::getAmountCents).sum();
                diffs.add(new ReconDiff(tradeId, ReconDiffType.DUPLICATE, bank.code(),
                        account, entryIds(onAccount), expected, netAmount));
                ok = false;
                continue;
            }

            JournalEntry entry = onAccount.getFirst();
            if (entry.getDirection() != expectedDir) {
                // 方向记反了：金额对得上，但借贷方向与角色不符
                diffs.add(new ReconDiff(tradeId, ReconDiffType.DIRECTION_MISMATCH, bank.code(),
                        account, List.of(entry.getEntryId()), expected, entry.getAmountCents()));
                ok = false;
                continue;
            }

            if (entry.getAmountCents() != expected) {
                diffs.add(new ReconDiff(tradeId, ReconDiffType.AMOUNT_MISMATCH, bank.code(),
                        account, List.of(entry.getEntryId()), expected, entry.getAmountCents()));
                ok = false;
            }
        }
        return ok;
    }

    /**
     * 分级处理 —— 文章第五节决策树的代码形态。
     *
     * <p>判断依据只有一条：正确的修正动作能不能由权威数据源（结算记录）唯一确定。
     * 能确定就自动做（漏记补记），不能确定就挂起等人裁决（重复、金额不符、方向不符）。
     */
    private void processDiff(ReconDiff diff, Map<String, SettlementRecord> settlementMap) {
        switch (diff.type()) {
            case MISSING -> {
                SettlementRecord s = settlementMap.get(diff.tradeId());
                if (s == null) {
                    // 没有权威源，补记的依据不存在 → 不自动动账
                    alert.p1(diff);
                    return;
                }
                compensateMissing(bankUrlOf(diff.bankCode()), diff, s);
                alert.autoFixed(diff);
            }
            case DUPLICATE -> {
                // 重复入账意味着幂等失效。tradeId 相同只能说明「多记了」，
                // 判断不出「哪一组是正确的、哪一组该冲正」——自动冲正有误冲正确记录的风险。
                // 所以只标记 + 给出候选，等人工裁决后调 /api/bank/fault/adjudicate-reverse。
                alert.manual(diff, "无法判断哪组多余，需人工裁决后冲正");
                // 只列候选、不替人选：把 entryIds 全填进命令会两组一起冲掉（净额归零），
                // 恰恰是错的。人工必须从候选里挑出「多余那组」再执行。
                log.warn("[recon] 候选分录（从中挑出多余那组，不要全选）: {}", diff.entryIds());
                log.warn("[recon] 人工裁决命令示例: curl -X POST {}/api/bank/fault/adjudicate-reverse "
                                + "-H 'Content-Type: application/json' "
                                + "-d '{\"tradeId\":\"{}\",\"entryIds\":[\"<多余那组的分录ID>\"]}'",
                        bankUrlOf(diff.bankCode()), diff.tradeId());
            }
            case AMOUNT_MISMATCH, DIRECTION_MISMATCH ->
                // 结算记录本身的金额也是参与方上报的，看不出哪边对；
                // 方向不符同样要回溯原始支付指令。两者都只能挂起。
                    alert.manual(diff, "需回溯原始支付指令确认，人工裁决后修正");
            case NO_SETTLEMENT ->
                // 结算侧无记录：可能是延迟、系统故障，也可能是欺诈交易。不自动处理。
                    alert.p1(diff);
            case IN_FLIGHT ->
                // 在途：数据可能尚未齐备，下一批次重新核对
                    alert.p2(diff);
        }
    }

    /** 通过 REST API 补记漏记的分录（正确动作完全由结算记录决定，没有歧义） */
    private void compensateMissing(String bankUrl, ReconDiff diff, SettlementRecord s) {
        try {
            restTemplate.postForEntity(bankUrl + "/api/bank/compensate",
                    Map.of("tradeId", diff.tradeId(),
                            "fromBank", s.getFromBank(),
                            "toBank", s.getToBank(),
                            "amountCents", s.getAmountCents(),
                            "settledAt", s.getSettledAt().toString()),
                    String.class);
            log.info("[recon] 补记完成: tradeId={}, 银行={}, 科目={}, 金额=¥{}.{}",
                    diff.tradeId(), bankLabel(diff.bankCode()), diff.account().label(),
                    s.getAmountCents() / 100, s.getAmountCents() % 100);
        } catch (Exception e) {
            log.error("[recon] 补记失败: tradeId={}, 银行={}", diff.tradeId(), diff.bankCode(), e);
        }
    }

    /**
     * 闭环验收：修复动作完成后，重新跑一遍完整匹配逻辑，确认差异真的消失了。
     *
     * <p>只有「本轮自动处理过」的差异参与闭环。挂起等人工的和等待下一批次的都不算失败——
     * 它们本来就不该在这一轮被修好，把它们算进失败会让「闭环失败」失去信号意义。
     *
     * <p>验收不能用「数一下分录条数」这种简化判断：冲正是追加式的，原始分录不会删除，
     * 冲正后条数不变。必须按净分录（尚未被冲正的原始分录）重新跑一遍匹配。
     */
    private void verifyClosedLoop(List<ReconDiff> diffs, Map<String, SettlementRecord> settlementMap,
                                  Instant start, Instant end) {
        List<ReconDiff> autoFixed = diffs.stream()
                .filter(d -> d.type() == ReconDiffType.MISSING
                        && settlementMap.containsKey(d.tradeId()))
                .toList();
        if (autoFixed.isEmpty()) {
            log.info("[recon] 本轮无自动处理的差异，跳过闭环验收");
            return;
        }

        Map<String, List<JournalEntry>> aRecheck = groupByTrade(fetchBankEntries(bankAUrl, start, end));
        Map<String, List<JournalEntry>> bRecheck = groupByTrade(fetchBankEntries(bankBUrl, start, end));

        List<String> remaining = new ArrayList<>();
        for (ReconDiff diff : autoFixed) {
            SettlementRecord s = settlementMap.get(diff.tradeId());
            Bank bank = Bank.fromCode(diff.bankCode());
            List<JournalEntry> now = (bank == Bank.ICBC ? aRecheck : bRecheck)
                    .getOrDefault(diff.tradeId(), List.of());

            List<ReconDiff> recheckDiffs = new ArrayList<>();
            boolean ok = checkBank(now, s, bank, diff.tradeId(), recheckDiffs);
            if (!ok) {
                remaining.add(diff.tradeId() + "/" + diff.account().label());
            }
        }

        if (remaining.isEmpty()) {
            log.info("[recon] 闭环通过: {} 条自动处理的差异已重新核对为平", autoFixed.size());
        } else {
            log.error("[recon] 闭环失败: {} 条差异补记后仍不平: {}", remaining.size(), remaining);
        }
    }

    /** 通过 REST API 拉取银行净分录 */
    private List<JournalEntry> fetchBankEntries(String baseUrl, Instant start, Instant end) {
        try {
            ResponseEntity<List<JournalEntry>> resp = restTemplate.exchange(
                    baseUrl + "/api/bank/entries?start={start}&end={end}",
                    HttpMethod.GET, null,
                    new ParameterizedTypeReference<>() {},
                    start.toString(), end.toString());
            return resp.getBody() != null ? resp.getBody() : List.of();
        } catch (Exception e) {
            log.warn("[recon] 拉取银行数据失败: url={}, error={}", baseUrl, e.getMessage());
            return List.of();
        }
    }

    private static Map<String, List<JournalEntry>> groupByTrade(List<JournalEntry> entries) {
        return entries.stream().collect(Collectors.groupingBy(JournalEntry::getTradeId));
    }

    private static List<String> entryIds(List<JournalEntry> entries) {
        return entries.stream().map(JournalEntry::getEntryId).toList();
    }

    private String bankUrlOf(String bankCode) {
        return Bank.ICBC.code().equals(bankCode) ? bankAUrl : bankBUrl;
    }

    private static String bankLabel(String bankCode) {
        Bank b = Bank.fromCode(bankCode);
        return b != null ? b.label() : bankCode;
    }
}
