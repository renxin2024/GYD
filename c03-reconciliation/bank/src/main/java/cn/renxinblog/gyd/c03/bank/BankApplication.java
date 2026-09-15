package cn.renxinblog.gyd.c03.bank;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * 银行服务入口 —— 同一份代码起两次，扮演文章里的付款行（工行）和收款行（建行）。
 *
 * <ul>
 *   <li>{@code --spring.profiles.active=bank-a} → 工行 ICBC，端口 18081，库 gyd_c03_bank_a</li>
 *   <li>{@code --spring.profiles.active=bank-b} → 建行 CCB，端口 18082，库 gyd_c03_bank_b</li>
 * </ul>
 *
 * <p>三个注解各管一件事，是这次「按进程拆模块」的关键：
 * <ul>
 *   <li>{@code scanBasePackages = "...c03"} —— 组件扫描要覆盖到 {@code shared.mq} 里的
 *       {@code MqCommonConfig}（MQ 拓扑与消息转换器）。主类在 {@code ...c03.bank} 下，
 *       默认只扫本包，够不到 shared，所以显式放宽到整个 c03。</li>
 *   <li>{@code @EntityScan("...shared.ledger")} —— <b>只认账本契约这一个包</b>。
 *       shared 里还躺着 {@code shared.clearing.SettlementRecord}（结算侧的实体），
 *       银行进程必须无视它，否则 ddl-auto 会在银行库里建出一张永远用不上的
 *       settlement_record 表。放宽组件扫描的同时收窄实体扫描，两者缺一不可。</li>
 *   <li>{@code @EnableJpaRepositories("...c03.bank")} —— 银行只扫本模块的
 *       {@code JournalEntryRepository}；结算侧的 {@code SettlementRepository} 不在
 *       本模块 classpath 上，本来就引用不到。</li>
 * </ul>
 *
 * <p>代码里已经没有 {@code @Profile} 了：模块边界替代了它。profile 现在只用来
 * 选「这个实例是谁」（见 application-bank-a/b.yml），不再决定「哪段代码跑」。
 */
@SpringBootApplication(scanBasePackages = "cn.renxinblog.gyd.c03")
@EntityScan("cn.renxinblog.gyd.c03.shared.ledger")
@EnableJpaRepositories("cn.renxinblog.gyd.c03.bank")
public class BankApplication {

    public static void main(String[] args) {
        SpringApplication.run(BankApplication.class, args);
    }
}
