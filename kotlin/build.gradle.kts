import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    kotlin("jvm") version "1.3.71"
    java
    id("com.github.johnrengelman.shadow") version "5.2.0"
}

repositories {
    mavenLocal()
    jcenter()
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
}

dependencies {
    val ktorVersion = "1.3.1"
    val slf4jVersion = "1.7.30"

    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))

    implementation("com.github.doyaaaaaken:kotlin-csv-jvm:0.10.1")
    implementation("io.github.microutils:kotlin-logging:1.7.9")
    implementation("io.ktor:ktor-client-apache:${ktorVersion}") {
        exclude("commons-logging", "commons-logging")
    }
    implementation("io.ktor:ktor-client-core:${ktorVersion}")
    implementation("org.graalvm.sdk:graal-sdk:20.1.0")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.7")
    implementation("org.xerial:sqlite-jdbc:3.31.1")

    runtimeOnly("org.slf4j:jcl-over-slf4j:${slf4jVersion}")
    runtimeOnly("org.slf4j:slf4j-jdk14:${slf4jVersion}")

    compileOnly("com.oracle.substratevm:svm:19.2.1")

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
}

tasks.withType<ShadowJar> {
    manifest.attributes.apply {
        put("Main-Class", "my.edu.clhs.banktivity.stockquotesync.AppKt")
    }
}
