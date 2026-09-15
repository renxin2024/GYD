package cn.renxinblog.gyd.c03.shared.domain;

/**
 * 银行 —— 把 demo 里同一件事的四套叫法收拢到一个枚举。
 *
 * <p>之前代码里 {@code bank-a} / {@code ICBC} / {@code 工行} / {@code gyd_c03_bank_a}
 * 分散在 profile 名、配置、日志、数据库名四处，读者要自己在脑子里做映射。
 * 这里集中定义，各处按需取对应的值：
 *
 * <ul>
 *   <li>{@link #profile()} —— Spring profile 名（bank-a / bank-b），启动服务用</li>
 *   <li>{@link #code()} —— 结算消息与分录里的银行代码（ICBC / CCB）</li>
 *   <li>{@link #label()} —— 中文名（工商银行 / 建设银行），日志用</li>
 *   <li>{@link #database()} —— 各自的 PostgreSQL 库名</li>
 * </ul>
 *
 * <p>在文章场景里，工行是付款行（A 的开户行），建行是收款行（B 的开户行）。
 */
public enum Bank {

    /** 工商银行：文章场景里的付款行，A 的开户行 */
    ICBC("bank-a", "ICBC", "工商银行", "gyd_c03_bank_a", "工行"),

    /** 建设银行：文章场景里的收款行，B 的开户行 */
    CCB("bank-b", "CCB", "建设银行", "gyd_c03_bank_b", "建行");

    private final String profile;
    private final String code;
    private final String fullName;
    private final String database;
    private final String shortLabel;

    Bank(String profile, String code, String fullName, String database, String shortLabel) {
        this.profile = profile;
        this.code = code;
        this.fullName = fullName;
        this.database = database;
        this.shortLabel = shortLabel;
    }

    public String profile() {
        return profile;
    }

    /** 银行代码，与结算记录里的 fromBank / toBank 对应 */
    public String code() {
        return code;
    }

    /** 中文全称（工商银行 / 建设银行） */
    public String fullName() {
        return fullName;
    }

    /** 中文简称，日志里用（工行 / 建行） */
    public String label() {
        return shortLabel;
    }

    /** 各自的 PostgreSQL 库名 */
    public String database() {
        return database;
    }

    /** 按银行代码（ICBC / CCB）查枚举，找不到返回 null */
    public static Bank fromCode(String code) {
        for (Bank b : values()) {
            if (b.code.equals(code)) {
                return b;
            }
        }
        return null;
    }
}
