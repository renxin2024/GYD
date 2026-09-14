// 根工程：统一镜像源 + 全子模块共享的插件版本（apply false，由子模块按需 apply）
// 子模块在各自 build.gradle.kts 里声明依赖与主类。
plugins {
    id("org.springframework.boot") version "4.1.1" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

// 用 subprojects 而不是 allprojects：根工程自己不写代码、不发包，拿到 group 只会让
// IDEA 多生成一个「<group>.<工程名>」形态的模块（cn.renxinblog.gyd.gyd），和其余
// 「gyd.<路径>」形态的模块命名对不上。GYA-Java 的根工程本就没有 group，两侧行为一致。
subprojects {
    group = "cn.renxinblog.gyd"
    version = "1.0.0"

    repositories {
        // 国内镜像源（阿里云 Maven，加速依赖下载；无需科学上网）
        maven { url = uri("https://maven.aliyun.com/repository/public") }
        maven { url = uri("https://maven.aliyun.com/repository/central") }
        mavenCentral()
    }
}
