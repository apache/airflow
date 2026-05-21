<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# ADR-0003: Pure Java DAGs — Build-Time Packaging and Code Visibility

## Status

Proposed

> **Note:** This ADR describes pure Java DAG authoring (entire DAGs written in Java without a
> Python file), which was removed from the scope of
> [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ). Per AIP-108, Java tasks are
> declared as `@task.stub` in ordinary Python DAG files; pure Java DAG authoring is left to a
> future proposal, likely after [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg)
> stabilises. The `BundleBuilder` interface and `Bundle`/`BundleBuilder.getDags()` mechanism
> remain in the SDK as the internal registry that the task execution runtime uses to locate task
> classes — they are not a public DAG-authoring surface in the current release.

## Context

[ADR-0001](0001-java-sdk-airflow-integration.md) originally introduced two ways to integrate non-Python tasks: `@task.stub` (mixed Python+Java DAGs) and pure Java DAGs (entire DAG in Java via `BundleBuilder`). [ADR-0004](0004-dag-parsing.md) and [ADR-0002](0002-workload-execution.md) describe the coordinator infrastructure for DAG parsing and task execution respectively.

This ADR focuses on the Java-SDK-specific concerns that would make pure Java DAGs work end-to-end — build-time metadata generation, source code packaging for UI visibility, and JAR manifest conventions — rather than the shared coordinator infrastructure already covered in those ADRs.

The central challenge is that Airflow Core expects to read DAG metadata and source code from files on disk or from the metadata DB. A JAR is an opaque binary — Airflow cannot `open()` it and read Python source. The Java SDK would need to bridge this gap at build time by embedding machine-readable metadata and human-readable source into the JAR itself.

## Decision

### JAR Manifest Conventions

The JAR manifest (`META-INF/MANIFEST.MF`) carries three SDK-specific attributes that Airflow and the Java SDK use to bootstrap a bundle:

| Attribute | Example Value                                     | Purpose |
|---|---------------------------------------------------|---|
| `Main-Class` | `org.apache.airflow.example.ExampleBundleBuilder` | Standard Java attribute; the coordinator uses it to launch the JVM |
| `Airflow-Java-SDK-Metadata` | `airflow-metadata.yaml`                           | Points to the embedded metadata file (dag IDs, task IDs) |
| `Airflow-Java-SDK-Dag-Code` | `JavaExampleBuilder.java`                         | Points to the embedded source file for Airflow UI display |

These attributes are set in the Gradle build (see [Build-Time Packaging](#build-time-packaging-gradle) below). The Python-side coordinator reads `Main-Class` to construct the launch command; `BundleScanner` reads `Airflow-Java-SDK-Metadata` to discover DAG IDs without launching the JVM.

### Build-Time Metadata: `airflow-metadata.yaml`

At build time, the SDK runs `BundleInspector` — a build-time utility that reflectively instantiates the user's `BundleBuilder` class, calls `getDags()`, and writes a YAML file listing every DAG ID and its task IDs:

```yaml
dags:
  java_example:
    tasks:
      - extract
      - transform
      - load
```

This file is embedded in the JAR root and referenced by the `Airflow-Java-SDK-Metadata` manifest attribute.

**Why build-time, not runtime?** The metadata must be available before the JVM starts. `BundleScanner` reads it from the JAR to discover which DAG IDs a bundle contains — this is used for `@task.stub` routing (mapping a `dag_id` to the correct bundle's classpath) without paying JVM startup cost. For pure Java DAGs, the coordinator already knows the bundle path, but the metadata is still useful for validation and tooling.

**`BundleInspector`:**

```kotlin
object BundleInspector {
  @JvmStatic
  fun main(args: Array<String>) {
    val className = args[0]
    val outputPath = args[1]
    val clazz = Class.forName(className)
    val instance = clazz.getDeclaredConstructor().newInstance() as? BundleBuilder
        ?: error("$className does not implement BundleBuilder")
    val dags = instance.getDags()
    File(outputPath).apply { parentFile.mkdirs() }.writeText(toYaml(dags))
  }

  internal fun toYaml(dags: List<Dag>): String = buildString {
    appendLine("dags:")
    for (dag in dags) {
      appendLine("  ${dag.dagId}:")
      appendLine("    tasks:")
      for (taskId in dag.tasks.keys) {
        appendLine("      - $taskId")
      }
    }
  }
}
```

### Source Code Packaging for UI Visibility

Airflow stores DAG source code in the `dag_code` table and displays it in the web UI. For Python DAGs this is trivial — `DagCode.write_code()` reads the `.py` file from disk. For a JAR, the raw bytecode is not human-readable.

The solution: pack the original `.java` source file into the JAR at build time. The `Airflow-Java-SDK-Dag-Code` manifest attribute tells the coordinator which file to extract.

On the Python side, `get_code_from_file()` on the coordinator:

1. Opens the JAR as a ZIP
2. Reads the `Airflow-Java-SDK-Dag-Code` attribute from the manifest
3. Extracts and returns the raw `.java` source

This lets Airflow's existing `DagCode` infrastructure store and display Java source code with no changes to Airflow Core.

### Build-Time Packaging (Gradle)

The `example/build.gradle.kts` shows the complete packaging pattern:

```kotlin
val bundleMainClass = application.mainClass.get()
val metadataFileName = "airflow-metadata.yaml"
val metadataOutputDir = layout.buildDirectory.dir("airflow-metadata")
val dagCodeSourcePath = bundleMainClass.replace('.', '/') + ".java"
val dagCodeFileName = bundleMainClass.substringAfterLast('.') + ".java"

// 1. Run BundleInspector at compile time to generate metadata
val inspectBundle = tasks.register<JavaExec>("inspectBundle") {
    dependsOn("classes")
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("org.apache.airflow.sdk.BundleInspector")
    args = listOf(bundleMainClass, metadataOutputDir.get().file(metadataFileName).asFile.absolutePath)
}

// 2. Pack metadata + source into the JAR
tasks.withType<Jar> {
    dependsOn(inspectBundle)
    from(metadataOutputDir)                    // airflow-metadata.yaml
    from("src/java/$dagCodeSourcePath")        // raw .java source file
    manifest {
        attributes(
            "Main-Class" to bundleMainClass,
            "Airflow-Java-SDK-Version" to project.version,
            "Airflow-Java-SDK-Metadata" to metadataFileName,
            "Airflow-Java-SDK-Dag-Code" to dagCodeFileName,
        )
    }
}
```

The resulting JAR contains:

```
example.jar
├── META-INF/MANIFEST.MF          (Main-Class, SDK attributes)
├── airflow-metadata.yaml          (dag IDs + task IDs)
├── JavaExampleBuilder.java               (raw source for UI display)
├── org/apache/airflow/example/
│   ├── JavaExampleBuildser.class          (compiled bundle entry point)
│   ├── JavaExampleBuilder$Extract.class
│   ├── JavaExampleBuilder$Transform.class
│   └── JavaExampleBuilder$Load.class
└── ...                            (SDK + dependency classes)
```

### `BundleScanner` — Runtime Bundle Discovery

`BundleScanner` reads JAR manifests at runtime to discover bundles without launching the JVM. This is used by the `@task.stub` path to resolve which bundle contains a given `dag_id`.

```kotlin
data class ResolvedBundle(
  val mainClass: String,   // From Main-Class manifest attribute
  val classpath: String,   // All JARs in bundle directory, colon-separated
)

fun scanBundles(bundlesDir: Path): Map<String, ResolvedBundle>
```

It supports two directory layouts:

- **Nested**: each subdirectory of `bundlesDir` is a bundle home (e.g., `bundles/my-app/lib/*.jar`)
- **Flat**: `bundlesDir` itself contains the JARs (e.g., `bundles/*.jar`)

For each JAR, it reads the `Airflow-Java-SDK-Metadata` manifest attribute, extracts the referenced YAML, parses DAG IDs, and returns a mapping from `dag_id` to `ResolvedBundle`.

### The BundleBuilder Authoring API

Bundle authors implement builder classes to define their DAGs:

```java
public class JavaExampleBuilder {

  public static class Extract implements Task {
    public void execute(Client client) throws Exception {
      var connection = client.getConnection("test_http");
      client.setXCom(new Date().getTime());
    }
  }

  public static class Transform implements Task {
    public void execute(Client client) {
      var extract_xcom = client.getXCom("extract");
      client.setXCom(new Date().getTime());
    }
  }

  public static Dag build() {
    var dag = new Dag("java_example", null, "@daily");
    dag.addTask("extract", Extract.class, List.of());
    dag.addTask("transform", Transform.class, List.of("extract"));
    return dag;
  }
}
```

and then collect DAGs with a BundleBuilder:

```java
public class ExampleBundleBuilder implements BundleBuilder {
    public Iterable<Dag> getDags() {
        return List.of(JavaExampleBuilder.build())
    }

    public static void main(String[] args) {
        var bundle = new ExampleBundleBuilder().build();
        Server.create(args).serve(bundle);
    }
}
```

The `main()` method is the JVM entry point that the coordinator launches. It wires the `BundleBuilder` to the SDK's TCP communication layer (`Server` → `CoordinatorComm`), which handles DAG parsing requests and task execution commands as described in [ADR-0004](0004-dag-parsing.md) and [ADR-0002](0002-workload-execution.md).

> **Note:** The current `BundleBuilder` interface is subject to review before the SDK reaches 1.0. Subclassing `Dag` directly may be a more natural fit and is being considered for post-OSS-integration.

### Deployment and Updates

A reasonable concern about JAR-based DAGs is whether updating a bundle requires draining or restarting the DAG processor / workers — Python source files are flexible because everything is read fresh on each parse, but a long-lived JVM holding a JAR open could pin an old version.

The design avoids this by leaning on the same ephemerality that Python uses:

- **DAG processor.** `DagFileProcessorManager` is long-lived, but each `DagFileProcessorProcess` child is one-shot and exits after returning a `DagFileParseRequest`. The Java runtime spawned underneath it (`java -classpath <bundle>/* …`) shares that lifetime — it loads the JAR fresh on every parse, then exits. Replacing the JAR on disk takes effect on the next scheduled parse with no manager restart.
- **Workers.** Each task instance launches its own JVM ([ADR-0002 — Runtime Lifecycle and Worker Capability](0002-workload-execution.md#runtime-lifecycle-and-worker-capability)). The classloader is process-scoped; a swapped JAR is picked up the next time a task starts. There is no warm JVM pool to invalidate.

In practice, "updating a Java DAG bundle" is the same shape as "updating a Python DAG file": drop the new file (or directory of JARs) into the bundle location and let normal scheduling pick it up. The version that runs a given task instance is determined at task start, not at worker start.

Two operational details worth flagging:

- **Atomic swap.** Writing a JAR in place while a task happens to be loading it can yield a corrupted read. Operators should prefer the standard "write to a temp name, rename into place" pattern, which the file system handles atomically on POSIX. This is the same guidance that already applies to Python file-system bundles.
- **Mid-run version skew.** Because the version is resolved per task launch, a long-running DAG run can in principle observe one bundle version for an upstream task and a different version for a downstream task if a swap happens between them. Bundle-version validation against `Airflow-Java-SDK-Bundle-Version` (planned — distinct from `Airflow-Java-SDK-Version`, which identifies the SDK toolkit; see [ADR-0002](0002-workload-execution.md#runtime-lifecycle-and-worker-capability)) gives operators a way to detect skew if it matters; the data-plane consequences (XCom shape changes, etc.) are the bundle author's responsibility, exactly as with Python.

## Consequences

- JAR bundles are self-contained: metadata, source, and compiled code are all in one artifact, simplifying deployment (copy one directory of JARs).
- Build-time metadata generation means DAG IDs can be discovered without JVM startup — important for `BundleScanner` and tooling.
- Source code packaging enables Airflow UI display with no changes to Airflow Core's `DagCode` infrastructure.
- The manifest convention (`Airflow-Java-SDK-*` attributes) is extensible — future attributes can carry additional metadata without breaking existing tooling.
- The build-time `BundleInspector` step adds a compile-time dependency on the SDK and requires the `BundleBuilder` class to be instantiable without side effects (no I/O, no connections in the constructor).
- Bundle authors must follow the Gradle packaging pattern (or replicate it in Maven/other build tools) — this is SDK-specific boilerplate that doesn't exist for Python DAGs.
