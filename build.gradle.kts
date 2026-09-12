// 根工程：统一镜像源 + 全子模块共享的插件版本（apply false，由子模块按需 apply）
// 子模块在各自 build.gradle.kts 里声明依赖与主类。
plugins {
    id("org.springframework.boot") version "4.1.1" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

allprojects {
    group = "cn.renxinblog.gyd"
    version = "1.0.0"

    repositories {
        // 国内镜像源（阿里云 Maven，加速依赖下载；无需科学上网）
        maven { url = uri("https://maven.aliyun.com/repository/public") }
        maven { url = uri("https://maven.aliyun.com/repository/central") }
        mavenCentral()
    }
}
