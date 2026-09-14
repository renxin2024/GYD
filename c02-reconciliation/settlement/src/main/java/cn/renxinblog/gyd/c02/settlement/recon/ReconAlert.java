package cn.renxinblog.gyd.c02.settlement.recon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 对账差异的告警通知。
 *
 * <p>生产环境应该对 P0（漏记、重复、金额/方向不符）做电话或 IM 告警，
 * 对 P1（结算侧无记录，待查）做值班跟进，P2（在途）做日报聚合。这里仅输出日志。
 *
 * <p>告警分三种结局，对应文章第五节的三类处理：
 * <ul>
 *   <li>{@link #autoFixed} —— 已自动补记（正确动作由权威源唯一确定）</li>
 *   <li>{@link #manual} —— 挂起等人工裁决，附带「为什么不能自动处理」的理由</li>
 *   <li>{@link #p1} / {@link #p2} —— 等待下一批次，不做动作</li>
 * </ul>
 */
@Component
public class ReconAlert {

    private static final Logger log = LoggerFactory.getLogger(ReconAlert.class);

    private final String notificationUrl;

    public ReconAlert(@Value("${gyd.c02.recon.notification-url:}") String notificationUrl) {
        this.notificationUrl = notificationUrl;
    }

    /** 对账完成后输出简报 */
    public void brief(int totalTrades, int matched, int diffs) {
        log.info("[recon] 对账简报: 共 {} 笔 / 平 {} 笔 / 差异 {} 条", totalTrades, matched, diffs);
    }

    /** 已自动处理的差异（漏记 + 结算已确认 → 补记完成） */
    public void autoFixed(ReconDiff diff) {
        log.error("[recon][P0][已自动补记] {}", diff.describe());
    }

    /** 挂起等人工裁决的差异，附理由 */
    public void manual(ReconDiff diff, String reason) {
        log.error("[recon][P0][待人工裁决] {} | 原因: {}", diff.describe(), reason);
    }

    /** P1 差异告警（结算侧无记录，待查） */
    public void p1(ReconDiff diff) {
        log.warn("[recon][P1][待查] {} | 结算侧无已结算记录，等下一批次重新核对", diff.describe());
    }

    /** P2 差异（在途） */
    public void p2(ReconDiff diff) {
        log.info("[recon][P2][在途] {} | 数据可能尚未齐备，等下一批次重新核对", diff.describe());
    }
}
