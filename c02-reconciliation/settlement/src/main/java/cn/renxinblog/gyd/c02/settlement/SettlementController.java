package cn.renxinblog.gyd.c02.settlement;

import cn.renxinblog.gyd.c02.shared.clearing.SettlementRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 结算服务 REST 接口。
 *
 * POST /api/settle —— 发起一笔跨行转账，返回 tradeId
 * GET  /api/settlement/{tradeId} —— 查询清算结果
 */
@RestController
@RequestMapping("/api")
public class SettlementController {

    private static final Logger log = LoggerFactory.getLogger(SettlementController.class);

    private final SettlementService service;
    private final SettlementRepository repo;

    public SettlementController(SettlementService service, SettlementRepository repo) {
        this.service = service;
        this.repo = repo;
    }

    /** 发起一笔跨行转账 */
    @PostMapping("/settle")
    public Map<String, Object> settle(@RequestBody Map<String, Object> body) {
        String fromBank = (String) body.getOrDefault("fromBank", "ICBC");
        String toBank = (String) body.getOrDefault("toBank", "CCB");
        long amountCents = ((Number) body.getOrDefault("amountCents", 10000L)).longValue();

        String tradeId = service.settle(fromBank, toBank, amountCents);
        log.info("[api] 转账成功: tradeId={}, {}→{} {}分", tradeId, fromBank, toBank, amountCents);

        return Map.of("tradeId", tradeId, "status", "SETTLED");
    }

    /** 查询某笔的清算结果 */
    @GetMapping("/settlement/{tradeId}")
    public SettlementRecord getSettlement(@PathVariable String tradeId) {
        return repo.findByTradeId(tradeId);
    }
}