// RabbitMQ 架构与特性（GYD 系列第一话）。本篇**刻意使用裸客户端 amqp-client，
// 而不是 spring-boot-starter-amqp**。
//
// 理由：本篇要讲的对象模型正是 vhost / connection / channel / exchange / binding / queue
// 这六层，以及「声明被拒时被关掉的是 channel 还是 connection」。Spring AMQP 的
// RabbitTemplate 与 MessageListenerContainer 恰好把 connection 与 channel 这两层封装掉了，
// 用它反而看不到本篇要展示的东西（与 c04 选裸客户端同源）。
//
// 版本基线：amqp-client 5.30.0 —— 与 c02（spring-boot-starter-amqp，由 Boot 4.1.1 的 BOM
// 选定的版本）**同一个客户端版本**（已用 `:c02-mq-reliable-delivery:dependencyInsight
// --dependency amqp-client` 核实），避免版本差异引入新变量。
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
    implementation("com.rabbitmq:amqp-client:5.30.0")

    // 客户端内部日志走 slf4j 门面。默认没有绑定会静默成 NOP，而本篇有几处证据恰恰在
    // 客户端自己的 WARN 里（例如并发共享 channel 时确认序号错配的告警），所以显式绑一个
    // simple 实现，并把默认级别压到 warn：正文里引用的就是这些告警行。
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

application {
    // 用法：./gradlew :c01-rabbitmq-architecture:run --args="<demo>"
    // demo 取值见 DemoRunner 的帮助输出；末尾可再加 keep 保留资源。
    mainClass = "cn.renxinblog.gyd.rabbitmq.DemoRunner"
}

tasks.named<JavaExec>("run") {
    // 客户端（以及 slf4j-simple）的时间戳按本机时区打，便于和 docker logs 对齐
    systemProperty("org.slf4j.simpleLogger.showDateTime", "true")
    systemProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS")
    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")
}
