package cn.renxinblog.gyd.c03.bank;

import cn.renxinblog.gyd.c03.shared.domain.Bank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 本行标识配置。
 *
 * <p>bank-a profile 配 ICBC（工行，文章里的付款行），bank-b profile 配 CCB（建行，收款行）。
 * 配置里写银行代码，代码里通过 {@link #bank()} 拿到 {@link Bank} 枚举，
 * 中文名、profile、库名等都从枚举取，不再散落字符串。
 */
@Component
@ConfigurationProperties(prefix = "gyd.c03.bank")
public class BankProperties {

    /** 银行代码：ICBC / CCB */
    private String code = "ICBC";

    /** 本行消费的 RabbitMQ 队列名 */
    private String queue = "gyd.c03.settlement.bank-a";

    /** 把配置的银行代码解析成 {@link Bank} 枚举 */
    public Bank bank() {
        Bank b = Bank.fromCode(code);
        if (b == null) {
            throw new IllegalStateException("未知的银行代码: " + code + "（应为 ICBC 或 CCB）");
        }
        return b;
    }

    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }

    public String getQueue() { return queue; }
    public void setQueue(String queue) { this.queue = queue; }
}
