package cn.renxinblog.gyd.c02;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * GYD 系列第 2 篇（消息队列的可靠投递与可靠消费）的演示应用入口。
 *
 * 当前只是一个空框架：仅接入 Web 层，尚未接入 RocketMQ。
 */
@SpringBootApplication
public class GydC02Application {

    public static void main(String[] args) {
        SpringApplication.run(GydC02Application.class, args);
    }
}
