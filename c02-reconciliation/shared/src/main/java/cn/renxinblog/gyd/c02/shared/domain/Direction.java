package cn.renxinblog.gyd.c02.shared.domain;

/**
 * 借贷方向 —— 会计里的两个方向符号，相当于「左 / 右」，跟「借钱 / 贷款」没有关系。
 *
 * <p>五百年前意大利商人记账时定下的习惯，沿用至今。具体记哪边由
 * {@link Account#directionFor(Movement)} 按「科目性质 + 增减」推导，
 * 业务代码不直接写 DEBIT / CREDIT 的三元判断。
 */
public enum Direction {

    /** 借方 */
    DEBIT("借"),

    /** 贷方 */
    CREDIT("贷");

    private final String label;

    Direction(String label) {
        this.label = label;
    }

    /** 中文标签，用于日志和文章对照（借 / 贷） */
    public String label() {
        return label;
    }

    /** 反方向：冲正时把原分录的借改成贷、贷改成借 */
    public Direction opposite() {
        return this == DEBIT ? CREDIT : DEBIT;
    }
}
