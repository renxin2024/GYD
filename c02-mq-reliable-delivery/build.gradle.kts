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
    // Spring Boot 4 起 web starter 的正式名是 spring-boot-starter-webmvc，
    // 旧的 spring-boot-starter-web 仍可解析但已废弃，新代码统一用新名。
    implementation("org.springframework.boot:spring-boot-starter-webmvc")

    // RabbitMQ：Boot 4 里 AMQP starter 名保持不变（不像 web → webmvc 那样改名），
    // 版本由 Boot BOM 管理：Spring AMQP 4.1.1 + amqp-client 5.30.0。
    implementation("org.springframework.boot:spring-boot-starter-amqp")

    // Boot 4 的测试 starter 按技术拆分，webmvc-test 会传递引入 spring-boot-starter-test。
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
}

tasks.test {
    useJUnitPlatform()
}
