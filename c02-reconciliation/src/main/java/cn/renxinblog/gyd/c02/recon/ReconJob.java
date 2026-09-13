package cn.renxinblog.gyd.c02.recon;

import cn.renxinblog.gyd.c02.shared.Direction;
import cn.renxinblog.gyd.c02.shared.JournalEntry;
import cn.renxinblog.gyd.c02.settlement.SettlementRecord;
import cn.renxinblog.gyd.c02.settlement.SettlementRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
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
 * 核心流程：
 * <ol>
 *   <li>通过 REST API 拉取 bank_a 和 bank_b 的当日记账数据</li>
 *   <li>从本地 DB 拉取结算记录（权威数据源）</li>
 *   <li>以 tradeId 为键建立三方索引，逐笔匹配</li>
 *   <li>对每条差异做分级决策：在途等待/补记/冲正/人工裁决</li>
 *   <li>闭环验收：处理完后重新拉取数据确认差异清零</li>
 * </ol>
 *
 * 匹配规则：
 * <ul>
 *   <li>结算记录必须存在且状态=SETTLED</li>
 *   <li>付款行必须有对应的 DEBIT 分录（金额以结算记录为准）</li>
 *   <li>收款行必须有对应的 CREDIT 分录</li>
 *   <li>金额不作为唯一匹配依据，以 tradeId 为主键</li>
 *   <li>重复分录通过 tradeId + direction 计数判断</li>
 * </ul>
 *
 * 修复幂等：
 * 补记和冲正通过银行侧的 REST API 执行（POST /api/bank/compensate 和 POST /api/bank/reverse）。
 * bank 侧通过 entryId 唯一索引保证幂等。
 */
@Component
@Profile("settlement")
public class ReconJob {

    private static final Logger log = LoggerFactory.getLogger(ReconJob.class);

    private final SettlementRepository settlementRepo;
    private final ReconAlert alert;
    private final RestTemplate restTemplate;

    private final String bankAUrl;
    private final String bankBUrl;

    public ReconJob(SettlementRepository settlementRepo,
                    ReconAlert alert,
                    @Value("${gyd.c02.recon.bank-a-url:http://localhost:18081}") String bankAUrl,
                    @Value("${gyd.c02.recon.bank-b-url:http://localhost:18082}") String bankBUrl) {
        this.settlementRepo = settlementRepo;
        this.alert = alert;
        this.bankAUrl = bankAUrl;
        this.bankBUrl = bankBUrl;
        this.restTemplate = new RestTemplate();
    }

    /** 定时执行 */
    @Scheduled(fixedDelayString = "${gyd.c02.recon.schedule-ms:60000}",
            initialDelayString = "${gyd.c02.recon.initial-delay-ms:30000}")
    public void run() {
        LocalDate today = LocalDate.now();
        ZonedDateTime startZ = today.atStartOfDay(ZoneOffset.UTC);
        ZonedDateTime endZ = today.plusDays(1).atStartOfDay(ZoneOffset.UTC);
        Instant start = startZ.toInstant();
        Instant end = endZ.toInstant();

        log.info("[recon] 开始日终对账: {} → {}", start, end);

        // 1. 拉取三方数据
        List<SettlementRecord> settlements = settlementRepo.findSettledBetween(start, end);
        List<JournalEntry> bankAEntries = fetchBankEntries(bankAUrl, start, end);
        List<JournalEntry> bankBEntries = fetchBankEntries(bankBUrl, start, end);

        log.info("[recon] 拉取完成: 结算{}条, bank_a{}条, bank_b{}条",
                settlements.size(), bankAEntries.size(), bankBEntries.size());

        // 2-4: 同之前的逻辑（匹配 + 分级处理）
        Map<String, List<JournalEntry>> aByTrade = bankAEntries.stream()
                .collect(Collectors.groupingBy(JournalEntry::getTradeId));
        Map<String, List<JournalEntry>> bByTrade = bankBEntries.stream()
                .collect(Collectors.groupingBy(JournalEntry::getTradeId));

        Set<String> allTradeIds = new HashSet<>();
        settlements.forEach(s -> allTradeIds.add(s.getTradeId()));
        allTradeIds.addAll(aByTrade.keySet());
        allTradeIds.addAll(bByTrade.keySet());

        List<ReconDiff> diffs = new ArrayList<>();
        int matched = 0;
        Map<String, SettlementRecord> settlementMap = settlements.stream()
                .collect(Collectors.toMap(SettlementRecord::getTradeId, s -> s));

        for (String tradeId : allTradeIds) {
            SettlementRecord settlement = settlementMap.get(tradeId);
            List<JournalEntry> aEntries = aByTrade.getOrDefault(tradeId, List.of());
            List<JournalEntry> bEntries = bByTrade.getOrDefault(tradeId, List.of());

            if (settlement == null) {
                if (!aEntries.isEmpty())
                    diffs.add(new ReconDiff(tradeId, ReconDiffType.NO_SETTLEMENT, "ICBC",
                            ids(aEntries), -1, 0));
                if (!bEntries.isEmpty())
                    diffs.add(new ReconDiff(tradeId, ReconDiffType.NO_SETTLEMENT, "CCB",
                            ids(bEntries), -1, 0));
                continue;
            }

            boolean aOk = checkBank(aEntries, settlement.getAmountCents(),
                    settlement.getFromBank(), "ICBC", tradeId, diffs);
            boolean bOk = checkBank(bEntries, settlement.getAmountCents(),
                    settlement.getToBank(), "CCB", tradeId, diffs);

            if (aOk && bOk) matched++;
        }

        alert.brief(allTradeIds.size(), matched, diffs.size());

        // 5. 分级处理差异
        for (ReconDiff diff : diffs) {
            processDiff(diff, settlementMap);
        }

        // 6. 闭环验收：重新拉取净分录，复用完整匹配引擎重新核对
        //    （不能用 checkFixed 那种简化的「计数==1」判断——冲正后原始分录仍保留，
        //      只有按净分录重新跑一遍完整匹配，才能确认差异真正消失）
        List<JournalEntry> aRecheck = fetchBankEntries(bankAUrl, start, end);
        List<JournalEntry> bRecheck = fetchBankEntries(bankBUrl, start, end);
        Map<String, List<JournalEntry>> aR = aRecheck.stream()
                .collect(Collectors.groupingBy(JournalEntry::getTradeId));
        Map<String, List<JournalEntry>> bR = bRecheck.stream()
                .collect(Collectors.groupingBy(JournalEntry::getTradeId));

        List<String> remaining = new ArrayList<>();
        for (ReconDiff diff : diffs) {
            if (diff.type() == ReconDiffType.IN_FLIGHT
                    || diff.type() == ReconDiffType.NO_SETTLEMENT) continue;
            SettlementRecord s = settlementMap.get(diff.tradeId());
            if (s == null) continue;

            // 用与初次匹配相同的 checkBank 重新核对该银行侧的净分录
            List<JournalEntry> now = "ICBC".equals(diff.bankCode())
                    ? aR.getOrDefault(diff.tradeId(), List.of())
                    : bR.getOrDefault(diff.tradeId(), List.of());
            List<ReconDiff> recheckDiffs = new ArrayList<>();
            boolean ok = checkBank(now, s.getAmountCents(),
                    "ICBC".equals(diff.bankCode()) ? s.getFromBank() : s.getToBank(),
                    diff.bankCode(), diff.tradeId(), recheckDiffs);
            if (!ok) {
                remaining.add(diff.tradeId());
            }
        }

        if (remaining.isEmpty()) {
            log.info("[recon] 闭环通过: 所有可处理差异已修复");
        } else {
            log.error("[recon] 闭环失败: {} 条差异仍未处理: {}", remaining.size(), remaining);
        }
    }

    /** 通过 REST API 拉取银行分录 */
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

    private boolean checkBank(List<JournalEntry> entries, long settlementAmount,
                               String expectedBank, String bankCode,
                               String tradeId, List<ReconDiff> diffs) {
        if (!expectedBank.equals(bankCode)) return true;

        Direction expectedDir =
                "ICBC".equals(bankCode)
                        ? Direction.DEBIT
                        : Direction.CREDIT;

        if (entries.isEmpty()) {
            diffs.add(new ReconDiff(tradeId, ReconDiffType.MISSING, bankCode, List.of(), settlementAmount, 0));
            return false;
        }

        long dirCount = entries.stream().filter(e -> e.getDirection() == expectedDir).count();
        if (dirCount > 1) {
            List<String> dupIds = entries.stream()
                    .filter(e -> e.getDirection() == expectedDir)
                    .map(JournalEntry::getEntryId).toList();
            diffs.add(new ReconDiff(tradeId, ReconDiffType.DUPLICATE, bankCode, dupIds, settlementAmount, 0));
            return false;
        }
        if (dirCount == 0) {
            diffs.add(new ReconDiff(tradeId, ReconDiffType.MISSING, bankCode, List.of(), settlementAmount, 0));
            return false;
        }

        JournalEntry entry = entries.stream()
                .filter(e -> e.getDirection() == expectedDir).findFirst().orElseThrow();
        if (entry.getAmountCents() != settlementAmount) {
            diffs.add(new ReconDiff(tradeId, ReconDiffType.AMOUNT_MISMATCH, bankCode,
                    List.of(entry.getEntryId()), settlementAmount, entry.getAmountCents()));
            return false;
        }
        return true;
    }

    private void processDiff(ReconDiff diff, Map<String, SettlementRecord> settlementMap) {
        switch (diff.type()) {
            case MISSING -> {
                SettlementRecord s = settlementMap.get(diff.tradeId());
                if (s == null) { alert.p1(diff); return; }
                String bankUrl = "ICBC".equals(diff.bankCode()) ? bankAUrl : bankBUrl;
                compensateMissing(bankUrl, diff, s);
                alert.p0(diff);
            }
            case DUPLICATE -> {
                // 重复入账意味着幂等失效。此时无法可靠判断哪一组分录是「正确」的，
                // 自动冲正有误冲正确记录的风险——标记人工裁决，不自动处理。
                alert.p0(diff);
                log.warn("[recon] 重复入账，需人工裁决（无法判断哪组是多余）: tradeId={}, entries={}",
                        diff.tradeId(), diff.entryIds());
            }
            case AMOUNT_MISMATCH -> {
                alert.p0(diff);
                log.warn("[recon] 金额不符，需人工裁决: tradeId={}", diff.tradeId());
            }
            case NO_SETTLEMENT -> alert.p1(diff);
            case IN_FLIGHT -> alert.p2(diff);
        }
    }

    /** 通过 REST API 补记 */
    private void compensateMissing(String bankUrl, ReconDiff diff, SettlementRecord s) {
        try {
            restTemplate.postForEntity(bankUrl + "/api/bank/compensate",
                    Map.of("tradeId", diff.tradeId(),
                            "fromBank", s.getFromBank(),
                            "toBank", s.getToBank(),
                            "amountCents", s.getAmountCents(),
                            "settledAt", s.getSettledAt().toString()),
                    String.class);
            log.info("[recon] 补记完成: tradeId={} bank={}", diff.tradeId(), diff.bankCode());
        } catch (Exception e) {
            log.error("[recon] 补记失败: tradeId={}", diff.tradeId(), e);
        }
    }

    private static List<String> ids(List<JournalEntry> entries) {
        return entries.stream().map(JournalEntry::getEntryId).toList();
    }
}