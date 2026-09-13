package cn.renxinblog.gyd.c02.recon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * 对账差异的告警通知。
 *
 * 生产环境应该对 P0（金额偏差、漏记）做电话/IM 告警，
 * P2（在途、待查）做日报聚合。这里仅做日志输出。
 */
@Component
@Profile("settlement")
public class ReconAlert {

    private static final Logger log = LoggerFactory.getLogger(ReconAlert.class);

    private final String notificationUrl;

    public ReconAlert(@Value("${gyd.c02.recon.notification-url:}") String notificationUrl) {
        this.notificationUrl = notificationUrl;
    }

    /** 对账完成后输出简报 */
    public void brief(int totalTrades, int matched, int diffs) {
        log.info("[recon] 对账简报: 总{}笔/平{}笔/差异{}笔", totalTrades, matched, diffs);
    }

    /** P0 差异告警（漏记/重复/金额偏差） */
    public void p0(ReconDiff diff) {
        log.error("[recon][P0] {} | tradeId={} | bank={} | 结算金额={}分 | 银行金额={}分 | entries={}",
                diff.type(), diff.tradeId(), diff.bankCode(),
                diff.settlementAmountCents(), diff.bankAmountCents(), diff.entryIds());
    }

    /** P1 差异告警（待查） */
    public void p1(ReconDiff diff) {
        log.warn("[recon][P1] {} | tradeId={} | bank={}",
                diff.type(), diff.tradeId(), diff.bankCode());
    }

    /** P2 差异（在途） */
    public void p2(ReconDiff diff) {
        log.info("[recon][P2] {} | tradeId={} | bank={}",
                diff.type(), diff.tradeId(), diff.bankCode());
    }
}