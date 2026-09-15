// GYD（Get Your Distributed）系列配套代码 —— Gradle 多模块工程
// 每篇文章一个 cXX-<slug> 子模块。
pluginManagement {
    repositories {
        // 阿里云 Gradle 插件镜像（Spring Boot 插件走这里，无需科学上网）
        maven { url = uri("https://maven.aliyun.com/repository/gradle-plugin") }
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name = "gyd"

// c01：RabbitMQ 的架构与特性。**刻意用裸客户端 amqp-client**，不走 spring-boot-starter-amqp
// —— 本篇要看的正是 connection 与 channel 这两层，而 Spring AMQP 恰好把它们封装掉了。
// 理由见该模块 build.gradle.kts 顶部。
include("c01-rabbitmq-architecture")

// c02：消息队列的可靠投递与可靠消费。这一篇反过来用 spring-boot-starter-amqp：要看的
// 是 RabbitTemplate 与监听容器上的 confirm / return / ack / prefetch 开关。
include("c02-mq-reliable-delivery")

// c03：一篇文 = 一个聚合目录，下面按「进程」再分子模块（模块边界 = 进程边界）。
// 子模块目录刻意不再带 c03- 前缀：外层已经有 c03-reconciliation/，再带一次会让
// IDEA 生成出 gyd.c03-reconciliation.c03-bank 这种名字，c03 出现两次。
// 详见 c03-reconciliation/build.gradle.kts 顶部说明。
include("c03-reconciliation")
include("c03-reconciliation:shared")
include("c03-reconciliation:bank")
include("c03-reconciliation:settlement")

// c04：RocketMQ 架构与机制。用裸客户端（不是 rocketmq-spring-boot-starter），
// 客户端与服务端 apache/rocketmq:4.9.7 严格同版本 —— 理由见该模块 build.gradle.kts 顶部。
// 注意：该模块的**运行期资源名**（主题、消费组/生产者组）仍沿用录测时的 gyd-c03-*，改名会让
// README 里那些实测记录失真，故刻意冻结，详见该模块 README。
include("c04-rocketmq-architecture")
