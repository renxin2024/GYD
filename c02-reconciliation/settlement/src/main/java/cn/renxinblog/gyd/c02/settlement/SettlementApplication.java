package cn.renxinblog.gyd.c02.settlement;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 结算服务入口 —— 扮演文章里的「央行」角色，端口 18080，库 gyd_c02_settlement。
 *
 * <p>一个进程里同时跑两件事：
 * <ul>
 *   <li>结算：产出清算记录（对账的权威数据源），并通过 MQ 广播给两家银行记账</li>
 *   <li>对账 Job（{@code settlement.recon} 包）：日终把三方数据拉齐做逐笔匹配</li>
 * </ul>
 * 对账与结算同进程，所以权威源是直接查本地库的；银行那两方只能走 REST 拉。
 *
 * <p>与 {@code BankApplication} 对称的三个注解：
 * <ul>
 *   <li>{@code scanBasePackages = "...c02"} —— 覆盖到 {@code shared.mq.MqCommonConfig}
 *       与 {@code settlement.recon} 下的对账组件。</li>
 *   <li>{@code @EntityScan("...shared.clearing")} —— <b>只认清算契约</b>，无视
 *       {@code shared.ledger.JournalEntry}，否则结算库里会多出一张 journal_entry 表。</li>
 *   <li>{@code @EnableJpaRepositories("...c02.settlement")} —— 只扫本模块的
 *       {@code SettlementRepository}。</li>
 * </ul>
 *
 * <p>{@code @EnableScheduling} 供对账 Job 的 {@code @Scheduled} 定时触发用；
 * 跑实验时也可以调 {@code POST /api/recon/run} 同步触发一轮，不等定时器。
 */
@SpringBootApplication(scanBasePackages = "cn.renxinblog.gyd.c02")
@EntityScan("cn.renxinblog.gyd.c02.shared.clearing")
@EnableJpaRepositories("cn.renxinblog.gyd.c02.settlement")
@EnableScheduling
public class SettlementApplication {

    public static void main(String[] args) {
        SpringApplication.run(SettlementApplication.class, args);
    }
}
