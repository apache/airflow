import com.diffplug.gradle.spotless.SpotlessExtension
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.3.0"
    id("com.diffplug.spotless") version "7.2.1" // Last version supporting JDK 11.
    id("org.jlleitschuh.gradle.ktlint") version "14.0.1"
}

allprojects {
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")

    repositories { mavenCentral() }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(11))
        }
        sourceCompatibility = JavaVersion.VERSION_11
    }
    kotlin { compilerOptions { jvmTarget = JvmTarget.JVM_11 } }

    configure<SpotlessExtension> {
        java {
            target("**/*.java")
            googleJavaFormat().formatJavadoc(false)
            trimTrailingWhitespace()
            endWithNewline()
        }
    }
}
