plugins {
    id("org.gradle.toolchains.foojay-resolver-convention").version("0.10.0")
}

rootProject.name = "airflow-java-sdk"
include("example", "plugin", "processor", "sdk")
