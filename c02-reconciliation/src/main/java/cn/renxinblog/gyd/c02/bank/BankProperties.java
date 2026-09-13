package cn.renxinblog.gyd.c02.bank;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * 银行标识配置。
 * bank-a（ICBC）和 bank-b（CCB）通过 application.yml 中不同的 profile 区分。
 */
@Component
@Profile("bank-a | bank-b")
@ConfigurationProperties(prefix = "gyd.c02.bank")
public class BankProperties {

    /** 银行代码：ICBC / CCB */
    private String code = "ICBC";

    /** 本行消费的 RabbitMQ 队列名 */
    private String queue = "gyd.c02.settlement.bank-a";

    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }

    public String getQueue() { return queue; }
    public void setQueue(String queue) { this.queue = queue; }
}