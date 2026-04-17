plugins {
    application
}

dependencies {
    implementation(project(":sdk"))
    implementation("org.slf4j:slf4j-simple:2.0.17")
}

sourceSets {
    main {
        java.srcDir("src/java")
    }
}

application {
    mainClass = "org.apache.airflow.example.JavaExample"
}

val bundleMainClass = application.mainClass.get()
val metadataFileName = "airflow-metadata.yaml"
val metadataOutputDir = layout.buildDirectory.dir("airflow-metadata")
val dagCodeSourcePath = bundleMainClass.replace('.', '/') + ".java"
val dagCodeFileName = bundleMainClass.substringAfterLast('.') + ".java"

val inspectBundle =
    tasks.register<JavaExec>("inspectBundle") {
        dependsOn("classes")
        classpath = sourceSets.main.get().runtimeClasspath
        mainClass.set("org.apache.airflow.sdk.BundleInspector")
        args =
            listOf(
                bundleMainClass,
                metadataOutputDir
                    .get()
                    .file(metadataFileName)
                    .asFile.absolutePath,
            )
    }

tasks.withType<Jar> {
    dependsOn(inspectBundle)
    from(metadataOutputDir)
    from("src/java/$dagCodeSourcePath")
    manifest {
        attributes(
            "Main-Class" to bundleMainClass,
            "Airflow-Java-SDK-Version" to project.version,
            "Airflow-Java-SDK-Metadata" to metadataFileName,
            "Airflow-Java-SDK-Dag-Code" to dagCodeFileName,
            "Implementation-Title" to "Example Java bundle",
            "Implementation-Version" to "1",
        )
    }
}
