package org.apache.airflow.sdk

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.util.jar.JarOutputStream
import java.util.jar.Manifest
import java.util.zip.ZipEntry

private const val STUB_MAIN_CLASS = "com.example.Main"

class BundleScannerTest {
  @Test
  @DisplayName("parseDagIdsFromYaml extracts dag ids from metadata YAML")
  fun parseDagIdsFromYaml() {
    val yaml =
      """
      dags:
        java_example:
          tasks:
            - extract
            - transform
            - load
        another_dag:
          tasks:
            - task_a
      """.trimIndent()

    assertEquals(setOf("java_example", "another_dag"), parseDagIdsFromYaml(yaml))
  }

  @Test
  @DisplayName("parseDagIdsFromYaml returns empty set for missing dags key")
  fun parseDagIdsFromYamlEmpty() {
    assertEquals(emptySet<String>(), parseDagIdsFromYaml("other_key: value"))
  }

  @Test
  @DisplayName("readBundleDagIds reads metadata from JAR with Airflow-Java-SDK-Metadata manifest")
  fun readBundleDagIdsFromJar(
    @TempDir tempDir: Path,
  ) {
    createBundleJar(tempDir, mapOf("my_dag" to listOf("t1", "t2")))

    assertEquals(setOf("my_dag"), readBundleDagIds(tempDir))
  }

  @Test
  @DisplayName("readBundleDagIds returns empty set when no JAR has metadata")
  fun readBundleDagIdsNoMetadata(
    @TempDir tempDir: Path,
  ) {
    val manifest = Manifest()
    manifest.mainAttributes.putValue("Manifest-Version", "1.0")
    JarOutputStream(Files.newOutputStream(tempDir.resolve("plain.jar")), manifest).use {}

    assertEquals(emptySet<String>(), readBundleDagIds(tempDir))
  }

  @Test
  @DisplayName("scanBundles discovers bundles in subdirectories")
  fun scanBundlesNestedLayout(
    @TempDir tempDir: Path,
  ) {
    val bundleA = Files.createDirectory(tempDir.resolve("bundle-a"))
    createBundleJar(bundleA, mapOf("dag_a" to listOf("t1")))

    val bundleB = Files.createDirectory(tempDir.resolve("bundle-b"))
    createBundleJar(bundleB, mapOf("dag_b" to listOf("t2"), "dag_c" to listOf("t3")))

    val result = scanBundles(tempDir)

    assertEquals(STUB_MAIN_CLASS, result["dag_a"]?.mainClass)
    assertEquals(STUB_MAIN_CLASS, result["dag_b"]?.mainClass)
    assertEquals(STUB_MAIN_CLASS, result["dag_c"]?.mainClass)
    // dag_a classpath should point to bundle-a JARs, not bundle-b
    assertTrue(result["dag_a"]!!.classpath.contains("bundle-a"))
    assertTrue(result["dag_b"]!!.classpath.contains("bundle-b"))
  }

  @Test
  @DisplayName("scanBundles supports flat layout where bundlesDir itself contains JARs")
  fun scanBundlesFlatLayout(
    @TempDir tempDir: Path,
  ) {
    createBundleJar(tempDir, mapOf("flat_dag" to listOf("t1")))

    val result = scanBundles(tempDir)

    assertNotNull(result["flat_dag"])
    assertEquals(STUB_MAIN_CLASS, result["flat_dag"]!!.mainClass)
  }

  @Test
  @DisplayName("scanBundles finds metadata JAR among many dependency JARs")
  fun scanBundlesFlatWithDependencyJars(
    @TempDir tempDir: Path,
  ) {
    // Simulate installDist layout: one bundle JAR with metadata among plain dependency JARs.
    val plainManifest = Manifest()
    plainManifest.mainAttributes.putValue("Manifest-Version", "1.0")
    JarOutputStream(Files.newOutputStream(tempDir.resolve("aaa-dep.jar")), plainManifest).use {}
    JarOutputStream(Files.newOutputStream(tempDir.resolve("zzz-dep.jar")), plainManifest).use {}

    // A JAR with no manifest at all.
    JarOutputStream(Files.newOutputStream(tempDir.resolve("no-manifest.jar"))).use {}

    createBundleJar(tempDir, mapOf("my_dag" to listOf("t1")))

    val result = scanBundles(tempDir)

    assertNotNull(result["my_dag"])
    assertEquals(STUB_MAIN_CLASS, result["my_dag"]!!.mainClass)
    // All 4 JARs should be on the classpath.
    val cpEntries = result["my_dag"]!!.classpath.split(File.pathSeparator)
    assertEquals(4, cpEntries.size)
  }

  @Test
  @DisplayName("scanBundles resolves distZip layout where bundlesDir is the lib directory")
  fun scanBundlesDistZipLibDir(
    @TempDir tempDir: Path,
  ) {
    // Simulate: unzip example.zip → example/lib/*.jar, BUNDLES_DIR=.../example/lib
    val libDir = Files.createDirectories(tempDir.resolve("example").resolve("lib"))

    // 30 plain dependency JARs
    val plainManifest = Manifest()
    plainManifest.mainAttributes.putValue("Manifest-Version", "1.0")
    for (name in listOf(
      "annotations-23.0.0",
      "converter-jackson-3.0.0",
      "jackson-core-2.21.1",
      "jackson-databind-2.21.1",
      "kotlin-stdlib-2.3.0",
      "kotlinx-coroutines-core-jvm-1.10.2",
      "msgpack-core-0.9.11",
      "okhttp-4.12.0",
      "retrofit-3.0.0",
      "sdk",
    )) {
      JarOutputStream(Files.newOutputStream(libDir.resolve("$name.jar")), plainManifest).use {}
    }

    // The bundle JAR with metadata, named "example.jar" (alphabetically after some deps)
    createBundleJar(libDir, mapOf("java_example" to listOf("extract", "transform", "load")), "example.jar")

    // bundlesDir points directly at lib/
    val result = scanBundles(libDir)

    assertNotNull(result["java_example"], "java_example should be discovered in flat lib/ layout")
    assertEquals(STUB_MAIN_CLASS, result["java_example"]!!.mainClass)
    assertEquals(11, result["java_example"]!!.classpath.split(File.pathSeparator).size)
  }

  @Test
  @DisplayName("scanBundles returns empty map for nonexistent directory")
  fun scanBundlesNonexistentDir() {
    assertEquals(emptyMap<String, ResolvedBundle>(), scanBundles(Path.of("/nonexistent/dir")))
  }

  private fun createBundleJar(
    dir: Path,
    dags: Map<String, List<String>>,
    fileName: String = "bundle.jar",
  ): Path {
    val manifest = Manifest()
    manifest.mainAttributes.putValue("Manifest-Version", "1.0")
    manifest.mainAttributes.putValue("Main-Class", STUB_MAIN_CLASS)
    manifest.mainAttributes.putValue(METADATA_MANIFEST_KEY, "airflow-metadata.yaml")

    val jarPath = dir.resolve(fileName)
    JarOutputStream(Files.newOutputStream(jarPath), manifest).use { jos ->
      jos.putNextEntry(ZipEntry("airflow-metadata.yaml"))
      val yaml =
        buildString {
          appendLine("dags:")
          for ((dagId, tasks) in dags) {
            appendLine("  $dagId:")
            appendLine("    tasks:")
            for (task in tasks) {
              appendLine("      - $task")
            }
          }
        }
      jos.write(yaml.toByteArray())
      jos.closeEntry()
    }
    return jarPath
  }
}
