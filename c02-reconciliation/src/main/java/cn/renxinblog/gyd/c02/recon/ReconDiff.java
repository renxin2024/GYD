package cn.renxinblog.gyd.c02.recon;

import java.util.List;

/**
 * 对账差异记录 —— 不可变的事实陈述。
 *
 * 「结算侧记录表明某笔交易成功了，但某家银行缺少/多记/记错了对应的分录。」
 * 这只是一个观察结果，不包含该做什么的判断——处理动作由 ReconJob 的分级决策模型决定。
 */
public record ReconDiff(
        String tradeId,
        ReconDiffType type,
        String bankCode,
        /** 对问题分录 entryId 的描述（可选） */
        List<String> entryIds,
        /** 结算侧的金额（分，作为基准） */
        long settlementAmountCents,
        /** 银行分录的金额（分，对不上时用于展示偏差） */
        long bankAmountCents
) {}