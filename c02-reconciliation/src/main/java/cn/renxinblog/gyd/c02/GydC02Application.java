package cn.renxinblog.gyd.c02;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * GYD 系列第 2 篇（跨行清算对账）的演示应用入口。
 *
 * 三个独立服务用不同的 Spring profile 和端口启动：
 * <ul>
 *   <li>结算服务（含对账 Job）：profile=settlement，端口 18080</li>
 *   <li>付款行（bank-a/ICBC）：profile=bank-a，端口 18081</li>
 *   <li>收款行（bank-b/CCB）：profile=bank-b，端口 18082</li>
 * </ul>
 *
 * 生产环境这三个 profile 各跑一个独立进程；
 * demo 中也可以分别启动三个终端，各指定不同的 --spring.profiles.active。
 *
 * 对账 Job（ReconJob）随结算服务一起启动——它需要直接访问结算记录表，
 * 银行数据通过 REST API 拉取。
 */
@SpringBootApplication
@EnableScheduling
public class GydC02Application {

    public static void main(String[] args) {
        SpringApplication.run(GydC02Application.class, args);
    }
}