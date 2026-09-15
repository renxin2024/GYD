package cn.renxinblog.gyd.c03.settlement.recon;

/**
 * 对账差异类型 —— 差异的分类标签，实际处理动作由 {@link ReconJob#processDiff} 按分级决策模型执行。
 *
 * <p>分类与处理动作的对应关系（文章第五节的决策树）：
 * <ul>
 *   <li>{@link #MISSING} 漏记 + 结算已确认 → <b>自动补记</b>（正确动作由权威源唯一确定）</li>
 *   <li>{@link #DUPLICATE} 重复入账 → <b>挂起等人工裁决</b>（判断不出该冲哪一组）</li>
 *   <li>{@link #AMOUNT_MISMATCH} 金额不符 → <b>挂起等人工裁决</b>（判断不出哪边是对的）</li>
 *   <li>{@link #DIRECTION_MISMATCH} 借贷方向记反 → <b>挂起等人工裁决</b>（要回溯原始支付指令）</li>
 *   <li>{@link #NO_SETTLEMENT} 结算侧无记录 → <b>不做动作</b>，等下一批次</li>
 *   <li>{@link #IN_FLIGHT} 在途 → <b>不做动作</b>，等下一批次</li>
 * </ul>
 *
 * <p>判定边界：{@code MISSING}、{@code DUPLICATE}、{@code AMOUNT_MISMATCH}、
 * {@code DIRECTION_MISMATCH} 由 {@code checkBank} 按科目逐个判定；{@code NO_SETTLEMENT}
 * 由主循环判定（结算侧查不到已结算记录）；{@code IN_FLIGHT} 在枚举与处理分支中存在，
 * 但当前匹配逻辑不会主动产出它——判定在途需要一套「交易发起时间 vs 对账截止点」的
 * 口径规则，属于对账口径设计，见文章第四节末尾的说明。
 */
public enum ReconDiffType {

    /** 结算已确认，但本行该科目缺少对应分录 */
    MISSING("漏记"),

    /** 同一 tradeId 在同一科目下有多条净分录（幂等失效导致的重复入账） */
    DUPLICATE("重复入账"),

    /** 分录金额与结算记录的金额不一致 */
    AMOUNT_MISMATCH("金额不符"),

    /** 分录的借贷方向与本行在该交易中的角色不符（金额对、方向反） */
    DIRECTION_MISMATCH("方向不符"),

    /** 结算侧无对应的已结算记录（可能尚未到账，也可能是欺诈） */
    NO_SETTLEMENT("结算侧无记录"),

    /** 分录在截止点附近，数据可能尚未齐备 */
    IN_FLIGHT("在途");

    private final String label;

    ReconDiffType(String label) {
        this.label = label;
    }

    /** 中文名，用于告警日志 */
    public String label() {
        return label;
    }
}
