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

// c02：一篇文 = 一个聚合目录，下面按「进程」再分子模块（模块边界 = 进程边界）。
// 子模块目录刻意不再带 c02- 前缀：外层已经有 c02-reconciliation/，再带一次会让
// IDEA 生成出 gyd.c02-reconciliation.c02-bank 这种名字，c02 出现两次。
// 详见 c02-reconciliation/build.gradle.kts 顶部说明。
include("c02-reconciliation")
include("c02-reconciliation:shared")
include("c02-reconciliation:bank")
include("c02-reconciliation:settlement")
