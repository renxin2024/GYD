package cn.renxinblog.gyd.c03.shared.domain;

/**
 * 科目 —— 账本上的分类抽屉。
 *
 * <p>银行账本上不写「张三」「李四」，而写「客户存款」「存放央行款项」这样的类别，
 * 每一类是一个科目。demo 里每家银行只有两个科目（各自独立的数据库，
 * 客户身份是隐含的：付款行的客户就是 A，收款行的客户就是 B）。
 *
 * <p>两个科目的性质正好相反，这是文章第一节「同一个结算账户在两个账本里性质相反」的落点：
 * <ul>
 *   <li>{@link #CUSTOMER_DEPOSIT}（客户存款）是<b>负债</b>——客户存进来的钱是银行欠客户的</li>
 *   <li>{@link #PBOC_SETTLEMENT}（存放央行款项）是<b>资产</b>——银行存在央行的钱，是央行欠银行的</li>
 * </ul>
 *
 * <p>命名说明：这里用 {@code PBOC_SETTLEMENT}（存放央行款项）而不是 {@code CBDC}。
 * CBDC 是央行数字货币（Central Bank Digital Currency），与「商业银行在央行的结算账户余额」
 * 是两回事，混用会让人以为这套账走的是数字人民币。
 */
public enum Account {

    /** 客户存款：银行欠客户的钱，属于负债 */
    CUSTOMER_DEPOSIT("customer_deposit", "客户存款", AccountType.LIABILITY),

    /** 存放央行款项：银行存在央行的结算账户余额，属于资产 */
    PBOC_SETTLEMENT("pboc_settlement", "存放央行款项", AccountType.ASSET);

    private final String code;
    private final String label;
    private final AccountType type;

    Account(String code, String label, AccountType type) {
        this.code = code;
        this.label = label;
        this.type = type;
    }

    /** 存进 journal_entry.account_id 的值 */
    public String code() {
        return code;
    }

    /** 中文名，用于日志（客户存款 / 存放央行款项） */
    public String label() {
        return label;
    }

    public AccountType type() {
        return type;
    }

    /**
     * 借贷方向规则 —— 文章第二节那张表的代码形态：
     *
     * <pre>
     *           增加记   减少记
     *   资产     借       贷
     *   负债     贷       借
     * </pre>
     *
     * <p>资产增加记借方、负债增加记贷方；反过来则相反。业务代码只说
     * 「客户存款减少了一万」，方向由这里推导，不在调用方写三元表达式。
     */
    public Direction directionFor(Movement movement) {
        boolean debitOnIncrease = (type == AccountType.ASSET);
        boolean increased = (movement == Movement.INCREASE);
        return (debitOnIncrease == increased) ? Direction.DEBIT : Direction.CREDIT;
    }

    /**
     * 分录的可读描述，用于日志与文章分录表对照。
     * 例：{@code 客户存款 减少（负债减少）→ 借}
     */
    public String describe(Movement movement) {
        return String.format("%s %s（%s%s）→ %s",
                label, movement.label(), type.label(), movement.label(),
                directionFor(movement).label());
    }

    /** 科目性质 */
    public enum AccountType {
        /** 资产：别人欠银行的钱 */
        ASSET("资产"),
        /** 负债：银行欠别人的钱 */
        LIABILITY("负债");

        private final String label;

        AccountType(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }
}
