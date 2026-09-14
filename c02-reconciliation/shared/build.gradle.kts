// shared —— 库模块，不可独立启动。
//
// 只放三方都要认的东西，且刻意不含任何 @Service/@Component 业务 bean：
//   shared.domain    领域词汇：科目、银行、借贷方向、记账规则（纯 Java，无 Spring 注解）
//   shared.ledger    账本契约：JournalEntry（银行之间、银行与对账之间用 REST 传）
//   shared.clearing  清算契约：SettlementRecord（结算 → 银行用 MQ 传）
//   shared.mq        MQ 拓扑：交换机/队列/绑定 + 消息转换器
//
// 为什么契约放这里而不是各自的业务模块：它们是跨进程边界的东西，
// 发送方和接收方都要依赖同一个定义，放在任一业务模块都会造成反向依赖。
//
// 实体分 ledger / clearing 两个子包不是凑数——两个可执行模块各用 @EntityScan
// 只扫自己那一个，从而每个库只建自己该有的表（详见各模块主类注释）。

plugins {
    `java-library`
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    // 用 api 而非 implementation：JournalEntry / SettlementRecord 是 @Entity，
    // 依赖方编译时需要 jakarta.persistence 注解；MqCommonConfig 用到 spring-amqp 类型。
    api("org.springframework.boot:spring-boot-starter-data-jpa")
    api("org.springframework.boot:spring-boot-starter-amqp")
}

// 库模块：不产出可执行 bootJar，只产出普通 jar 供其它模块依赖
tasks.withType<org.springframework.boot.gradle.tasks.bundling.BootJar> { enabled = false }
tasks.withType<Jar> { enabled = true }
