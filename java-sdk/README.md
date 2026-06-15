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

* Put the [DAG with stub tasks](./example/src/resources/dags) to somewhere Airflow can find.

* Ensure the `java` command is available in the same environment the Airflow
  task worker is in.

* Package the example to `./example/build/bundle`

  ```bash
  # We're now in the 'example' directory, so gradlew is in parent.
  ../gradlew bundle
  ```

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

## Technical Details

The user uses the SDK to implement a Java application that implements task
methods, and metadata on which DAG and task each method should be used
for.

When the Airflow Supervisor identifies a task should be run with Java, it
launches the Java application as a subprocess. The Java application accepts
flags `--comm` and `--logs` from the command line to identify TCP sockets it
should connect to, and communicates with the Supervisor through these channels
during execution.

1. On connection, the Supervisor immediately sends a StartupDetails message
   through the comm socket.
2. The Java application finds and executes the relevant method.
3. During execution, the Java application uses the comm socket to retrieve
   information (e.g. Variable) from, and send data (e.g. XCom) to Airflow.
4. The Java application informs the comm socket to tell the Supervisor the
   task's terminal state.
5. The Java application exits.

During the Java application's lifetime, it also sends log messages generated by
the SDK (not user code) through the logs socket, so the Supervisor can append
them to Airflow logs.

Communication uses the same formats as the Python-based processes.

See [Architectural Design Records](./adr) in the `adr` directory to learn more.
