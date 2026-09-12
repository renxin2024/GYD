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

include("c01-mq-reliable-delivery")
