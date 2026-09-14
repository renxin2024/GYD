package cn.renxinblog.gyd.c02.shared.domain;

/**
 * 余额变动方向 —— 这个科目的钱是多了还是少了。
 *
 * <p>和 {@link Direction}（借 / 贷）分开：Movement 说的是业务事实（减少了一万），
 * Direction 说的是记账符号（记借方）。两者的换算规则在
 * {@link Account#directionFor(Movement)}，也就是文章第二节那张方向规则表。
 */
public enum Movement {

    /** 余额增加 */
    INCREASE("增加"),

    /** 余额减少 */
    DECREASE("减少");

    private final String label;

    Movement(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }

    public Movement opposite() {
        return this == INCREASE ? DECREASE : INCREASE;
    }
}
