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

# Airflow Java SDK

A **JVM** SDK for Apache Airflow. You can use any JVM-compatible language to
write workflow bundles, and have Airflow consume the result.

The SDK and execution-time logic is implemented in Kotlin.
An example is bundled showing how the SDK can be used in Java.

## Building the SDK

```bash
./gradlew build
```

## Building documentation

```bash
./gradlew dokkaGenerate
```

This uses [Dokka](https://kotl.in/dokka) to build documentation of the Java SDK.
This generates both an HTML representation and Javadoc.


## Running the example

The SDK projects must first built and published:

```bash
./gradlew publishToMavenLocal -PskipSigning=true
```

After the build is successful, you should be able to see directories in `~/.m2/repository/org/apache/airflow/`.

Now `cd example` into the example project, and

* Package the example to `./example/build/bundle`

  ```bash
  # We're now in the 'example' directory, so gradlew is in parent.
  ../gradlew bundle
  ```

* Put the [DAG with stub tasks](./example/src/resources/dags) to somewhere Airflow can find.

* Ensure the `java` command is available in the same environment the Airflow
  task worker is in.

* Configure Airflow to route tasks in the *java* queue to be run with Java:

  ```bash
  export AIRFLOW__SDK__COORDINATORS='{
    "java": {
      "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
      "kwargs": {"jars_root": ["/opt/airflow/java-sdk/example/build/bundle"]}
    }
  }'
  export AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{"java": "java"}'
  ```

* Ensure the Connection and Variable needed by the example DAG are available:

  ```bash
  export AIRFLOW_CONN_TEST_HTTP='{
      "conn_type": "http",
      "login": "user",
      "password": "pass",
      "host": "example.com",
      "port": 1234,
      "extra": {"param1": "val1", "param2": "val2"}
  }'
  export AIRFLOW_VAR_MY_VARIABLE=123
  ```

## Publishing

The SDK is published to Maven Central via the
[ASF Nexus staging repository](https://repository.apache.org).
The full release process follows the
[ASF Maven publishing guide](https://infra.apache.org/publishing-maven-artifacts.html).

### Prerequisites

* An ASF committer account with access to
  [repository.apache.org](https://repository.apache.org).
* A GPG key that has been
  [added to the project KEYS file](https://infra.apache.org/release-signing.html)
  and uploaded to a public key server.

### Bump the version

Edit `gradle.properties` and set the version for this release:

```properties
projectVersion=1.0.0
```

Commit the change and push it to the release branch.

### Verify the POM locally

Before touching any remote repository, publish to your local Maven cache and
inspect the generated POM:

```bash
rm -rf ~/.m2/repository/org/apache/airflow/  # Start clean.

./gradlew publishToMavenLocal -PskipSigning=true

# The airflow-sdk runtime.
less ~/.m2/repository/org/apache/airflow/airflow-sdk/*/airflow-sdk-*.pom

# The bill of materials of airflow-sdk
less ~/.m2/repository/org/apache/airflow/airflow-sdk-bom/*/*.pom

# The annotation processor for the builder pattern.
less ~/.m2/repository/org/apache/airflow/airflow-sdk-processor/*/airflow-sdk-*.pom

# The Gradle plugin for bundling.
less ~/.m2/repository/org/apache/airflow/airflow-sdk-gradle-plugin/*/airflow-sdk-*.pom

# The Gradle plugin's registration.
less ~/.m2/repository/org/apache/airflow/sdk/org.apache.airflow.sdk.gradle.plugin/*/*.pom
```

Check that the coordinates, description, license, SCM, and organization fields
look correct.

### Dry-run against a local repository

To test the full publish flow without touching ASF infrastructure, override the
repository URL to a local directory

```bash
rm -rf /tmp/local-maven-repo  # Start clean.
./gradlew publish -PmavenUrl=file:///tmp/local-maven-repo -PskipSigning=true
ls /tmp/local-maven-repo/org/apache/airflow/
# This should contain the same components in ~/.m2 as inspected in the previous step.
```

*NOTE:* Signing is not required since nothing goes to Maven Central. If you want
to test signing, set the GPG private key and passphrase as described in the next
section, and remove `-PskipSigning=true` from the above command.

### Publish to ASF Nexus staging

Store the credentials in `~/.gradle/gradle.properties` so they are not exposed
in your shell history:

```properties
mavenUsername=your-asf-nexus-token-username
mavenPassword=your-asf-nexus-token-password
signing.password=your-gpg-key-passphrase
```

Then run the publish task.

```bash
./gradlew publish -P"signing.key=$(gpg --armor --export-secret-keys your-gpg-key-fingerprint)"
```

*NOTE:* The signing key is supplied through the command line since it contains
newlines, which does not work well in a Gradle properties file.

*NOTE:* You can also use the following environment variables to provide the
credentials instead: `ASF_NEXUS_USERNAME`, `ASF_NEXUS_PASSWORD`, `SIGNING_KEY`,
and `SIGNING_PASSWORD`. This is especially useful on e.g. CI.

### Verify the upload

Verify all artifacts have been released correctly to the
[ASF Nexus server](https://repository.apache.org/#nexus-search;quick~org.apache.airflow).

Check *Updated by* (should be your ID), *Uploaded Date*, and *Last Modified*.


## Contributing

The user implements a Java application containing task methods annotated (or
registered) with the SDK. The application is packaged as a bundle and placed
where Airflow can find it.

When the Airflow supervisor identifies that a task should run with Java, it
launches the JVM application as a subprocess. The flow is:

1. `JavaCoordinator.execute_task()` (Python) scans `jars_root`, builds the
   classpath, and spawns `java -cp <jars> <MainClass> --comm=<host>:<port>
   --logs=<host>:<port>`.
2. `Server.kt` connects to both sockets immediately on startup.
3. The supervisor sends a `StartupDetails` MessagePack message; the JVM reads
   it, looks up the matching task by `dag_id` + `task_id`, and calls the
   user's task method.
4. During execution the JVM sends requests to the supervisor (GetVariable,
   GetConnection, GetXCom, SetXCom, etc.) and the supervisor responds. All
   frames are a 4-byte big-endian length prefix followed by a MessagePack
   payload.
5. On completion (or exception) the JVM sends a `TaskState` message and closes
   the socket. The JVM process then exits.

Log messages produced by the SDK (not by user code) are forwarded over the
`--logs` socket so the supervisor can append them to Airflow's log store.

The wire protocol is defined in
`task-sdk/src/airflow/sdk/execution_time/schema/schema.json`.
`execution/Comm.kt` implements the framing layer. Adding a new message type
requires changes in **both** `schema.json` (Python side) and
`execution/Comm.kt` + `execution/Client.kt` (JVM side).

See [Architectural Design Records](./adr) in the `adr` directory to learn more.

### Repository layout

```
java-sdk/
├── sdk/          # Core library: public API (org.apache.airflow.sdk) and internal
│                 #   execution layer (org.apache.airflow.sdk.execution)
├── processor/    # Annotation processor that generates *Builder classes
├── plugin/       # Gradle plugin (org.apache.airflow.sdk) — bundle task, manifest
│                 #   attribute injection, and verifyBundleMainClass
├── bom/          # Bill of Materials POM so consumers can import all SDK artifacts
│                 #   at a consistent version
├── slf4j/        # SLF4J logging provider; routes SLF4J calls to the Airflow log store
├── jul/          # java.util.logging handler; routes JUL records to the Airflow log store
├── jpl/          # Java Platform Logging provider (System.Logger, JEP 264); routes JPL
│                 #   calls to the Airflow log store
├── log4j2/       # Log4j 2 appender; routes Log4j 2 events to the Airflow log store
├── example/      # End-to-end example bundle (annotation + interface APIs, Java source)
├── adr/          # Architectural Decision Records for the Java SDK
└── buildSrc/     # Shared Gradle convention plugins (Java version, lint, etc.)
```

The Python coordinator that launches the JVM subprocess lives outside this directory:

```
task-sdk/src/airflow/sdk/coordinators/java/   # JavaCoordinator (SubprocessCoordinator subclass)
task-sdk/tests/coordinators/java/             # Python-side unit and integration tests
```

### Testing

```bash
# Run all JVM tests
./gradlew test

# Run a specific test class
./gradlew :sdk:test --tests "org.apache.airflow.sdk.execution.CommTest"
```

For the Python coordinator, use Breeze (never run pytest on the host directly):

```bash
breeze testing task-sdk-tests -- task_sdk/coordinators/java
```

End-to-end tests that exercise a real Airflow environment:

```bash
E2E_TEST_MODE=java_sdk uv run --project airflow-e2e-tests pytest \
    tests/airflow_e2e_tests/java_sdk_tests/ -xvs
```

### Coding conventions

- All SDK and processor source is **Kotlin**; Java is the *public API target*,
  not the implementation language.
- Keep `sdk/src/main/kotlin/` (the public API surface) free of internal
  implementation details; those belong in the `execution/` sub-package.
- The annotation processor (`BuilderProcessor.kt`) uses `kapt`. When adding a
  new annotation, define it in `Builder.kt`, handle it in
  `BuilderProcessor.kt`, and add a golden-output test in
  `processor/src/test/kotlin/`.
- The Python coordinator subclasses `SubprocessCoordinator`. Do not reach into
  the JVM process from Python beyond what `_build_execute_task_command`
  provides.
- Run `./gradlew ktLintCheck spotlessCheck` (or `ktLintFormat spotlessApply`)
  before submitting — the project enforces Kotlin and Java formatting.
- All new files need the Apache License header.

### Common tasks

**Adding a new `Client` method** (e.g. a new Airflow API call):

1. Regenerate POJO classes from `schema.json` if the message type is new.
2. Add Kotlin request/response data classes in `execution/Comm.kt` or a new file.
3. Add the public-facing method to `Client.kt` which delegates to
   `execution/Client.kt` for the supervisor wire call.
4. Write a unit test in `sdk/src/test/kotlin/.../ClientTest.kt` mocking the
   socket layer.
5. Update `airflow-core/docs/authoring-and-scheduling/language-sdks/java.rst`
   if the change is user-visible.

**Adding a new annotation**:

1. Define the annotation interface in `Builder.kt`.
2. Handle it in `BuilderProcessor.kt` — generate the appropriate code in the
   `*Builder` class.
3. Add a test in `BuilderTest.kt` with expected generated output.
4. Update the annotation table in `java.rst`.

**Fixing a framing or protocol bug**: focus on `execution/Comm.kt` and
`execution/Frame.kt`. `CommTest.kt` covers encode/decode round-trips; add a
regression test reproducing the bug before fixing it.

### PR checklist

- Run `./gradlew build test` (JVM) and the relevant pytest suite (Python
  coordinator).
- Confirm the example bundle still compiles (see "Running the example" above up
  to the bundling step)
- If `schema.json` changed, verify both sides (JVM + Python) handle the
  new/changed fields.
- Add or update tests for every changed behaviour.
- For user-visible changes to `task-sdk/`, add a newsfragment under
  `airflow-core/newsfragments/`.
