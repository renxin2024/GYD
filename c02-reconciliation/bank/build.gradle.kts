// bank —— 银行服务，可执行模块。
//
// 同一份代码起两次：
//   --spring.profiles.active=bank-a → 工行 ICBC :18081，连 gyd_c02_bank_a
//   --spring.profiles.active=bank-b → 建行 CCB :18082，连 gyd_c02_bank_b
//
// 拆模块后 profile 的含义变了：它不再决定「哪段代码跑」（那是模块边界的事），
// 只决定「这个实例是谁」——银行代码、端口、库名、队列名。代码里已经没有 @Profile 了。

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
    implementation(project(":c02-reconciliation:shared"))

    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-amqp")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
}

springBoot {
    mainClass.set("cn.renxinblog.gyd.c02.bank.BankApplication")
}

tasks.test {
    useJUnitPlatform()
}
