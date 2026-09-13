package cn.renxinblog.gyd.c02.recon;

/**
 * 对账差异类型。
 *
 * 此枚举定义了差异的分类标签，实际处理动作由 ReconJob 按分级决策模型执行：
 * - 在途/待查 → 不做动作，等待下一批次
 * - 漏记 + 结算已确认 → 补记
 * - 重复入账 → 人工裁决（无法判断哪组多余，不能自动冲正）
 * - 金额不符 → 人工裁决
 */
public enum ReconDiffType {
    /** 结算已确认，付款行/收款行缺少对应的分录 */
    MISSING,
    /** 同 tradeId 下有重复分录（不含冲正记录） */
    DUPLICATE,
    /** 分录金额与结算记录的金额不一致 */
    AMOUNT_MISMATCH,
    /** 结算侧无对应的记录（可能尚未到账） */
    NO_SETTLEMENT,
    /** 分录在截止点附近，数据可能尚未齐备 */
    IN_FLIGHT
}