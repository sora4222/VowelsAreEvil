import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.40"
}

group = "com.sora4222.learning"
version = "1.0"

repositories {
    mavenCentral()
}
val beamVersion = "2.13.0"
dependencies {
    implementation(kotlin("stdlib-jdk8"))
    compile("org.apache.beam", "beam-sdks-java-core", beamVersion)
    compile("org.apache.beam", "beam-runners-direct-java", beamVersion)
    compile("org.apache.beam", "beam-sdks-java-io-kafka", beamVersion)
    compile( "org.slf4j", "slf4j-log4j12", "1.7.26")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}