// c03：RocketMQ 架构与机制。本篇用**裸客户端**而非 rocketmq-spring-boot-starter。
//
// 理由（与 plan 一致，写在这里方便后来者）：starter 最新仅 2.3.5、跟随 Spring Boot 2.x/3.x，
// 与本仓库基线 Boot 4.1.1 兼容性存疑；且本篇要演示的 TransactionMQProducer、
// MessageQueueSelector、MessageListenerOrderly 在 starter 里支持都很薄。
// 更要紧的是版本一致性：本篇要讲的行为（固定 18 级延迟、重试白名单、VIP 通道端口减 2）
// 都是版本相关的，客户端必须与服务端 apache/rocketmq:4.9.7 严格同版本。
plugins {
    java
    application
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}

dependencies {
    implementation("org.apache.rocketmq:rocketmq-client:4.9.7")
}

application {
    // 用法：./gradlew :c03-rocketmq-architecture:run --args="<demo>"
    // demo 取值见 DemoRunner 的帮助输出。
    mainClass = "cn.renxinblog.gyd.c03.DemoRunner"
}

tasks.named<JavaExec>("run") {
    // 客户端默认把日志写进 user.home 下的 logs/，这里统一落到模块内，方便随 demo 一起查看；
    // 该目录已被仓库根 .gitignore 的 logs/ 规则忽略。
    systemProperty("rocketmq.client.logRoot", layout.projectDirectory.dir("logs").asFile.absolutePath)
}
