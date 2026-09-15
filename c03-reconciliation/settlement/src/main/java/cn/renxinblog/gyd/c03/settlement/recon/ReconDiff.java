package cn.renxinblog.gyd.c03.settlement.recon;

import cn.renxinblog.gyd.c03.shared.domain.Account;

/**
 * 对账差异记录 —— 不可变的事实陈述。
 *
 * <p>它只描述「结算侧记录表明某笔交易成功了，但某家银行的某个科目上，分录缺失/多记/记错」，
 * 不包含「该做什么」的判断——处理动作由 {@link ReconJob#processDiff} 的分级决策模型决定。
 * 这个分离就是文章第八节讲的「对账确定了差多少，分级处理确定了怎么补回去」。
 *
 * @param tradeId               清算指令 ID（业务身份）
 * @param type                  差异类型
 * @param bankCode              哪家银行（ICBC 工行 / CCB 建行）
 * @param account               哪个科目（客户存款 / 存放央行款项）；NO_SETTLEMENT、IN_FLIGHT 这类
 *                              不针对具体科目的差异为 null
 * @param entryIds              涉及的分录业务流水号（漏记时为空；重复时列出全部候选，供人工裁决）
 * @param settlementAmountCents 结算侧金额（分，权威基准）；结算侧无记录时为 -1
 * @param bankAmountCents       银行分录金额（分，对不上时用于展示偏差）
 */
public record ReconDiff(
        String tradeId,
        ReconDiffType type,
        String bankCode,
        Account account,
        java.util.List<String> entryIds,
        long settlementAmountCents,
        long bankAmountCents
) {

    /** 兼容不针对具体科目的差异（NO_SETTLEMENT / IN_FLIGHT） */
    public ReconDiff(String tradeId, ReconDiffType type, String bankCode,
                     java.util.List<String> entryIds,
                     long settlementAmountCents, long bankAmountCents) {
        this(tradeId, type, bankCode, null, entryIds, settlementAmountCents, bankAmountCents);
    }

    /** 科目中文名，无科目时返回「整笔」 */
    public String accountLabel() {
        return account != null ? account.label() : "整笔";
    }

    /** 一行可读描述，用于告警日志 */
    public String describe() {
        return String.format("%s | tradeId=%s | %s 科目=%s | 结算=¥%s | 银行=¥%s | 分录=%s",
                type.label(), tradeId, bankCode, accountLabel(),
                settlementAmountCents < 0 ? "无" : String.format("%.2f", settlementAmountCents / 100.0),
                String.format("%.2f", bankAmountCents / 100.0),
                entryIds);
    }
}
