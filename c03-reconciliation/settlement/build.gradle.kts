// settlement —— 结算服务 + 对账 Job，可执行模块，端口 18080。
//
// 对账 Job 与结算服务同进程，所以能直接读本地结算库（权威源），
// 银行数据则走 REST 拉取——三方各持一账，谁也不能直连别人的库。

plugins {
    java
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation(project(":c03-reconciliation:shared"))

    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-amqp")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
}

springBoot {
    mainClass.set("cn.renxinblog.gyd.c03.settlement.SettlementApplication")
}

tasks.test {
    useJUnitPlatform()
}
