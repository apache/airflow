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

* Put the [DAG with stub tasks](./dags) to somewhere Airflow can find.

* Ensure the `java` command is available in the same environment the Airflow
  task worker is in.

* Package the example and its dependencies into JARs in
  `./example/build/install/example/lib`

  ```bash
  ./gradlew :example:installDist
  ```

* Configure Airflow to route tasks in the *java* queue to be run with Java:

  ```bash
  export AIRFLOW__SDK__COORDINATORS='{
    "java": {
      "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
      "kwargs": {"jars_root": ["/opt/airflow/java-sdk/example/build/install/example/lib"]}
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
sdkVersion=1.0.0
```

Commit the change and push it to the release branch.

### Verify the POM locally

Before touching any remote repository, publish to your local Maven cache and
inspect the generated POM:

```bash
./gradlew :sdk:publishToMavenLocal
cat ~/.m2/repository/org/apache/airflow/airflow-sdk/*/airflow-sdk-*.pom
```

Check that the coordinates, description, license, SCM, and organization fields
look correct.

### Export your signing key

The build expects an ASCII-armored PGP private key.  Export it with:

```bash
gpg --armor --export-secret-keys <your-key-id>
```

Copy the full output (including the header and footer) for use in the next step.

### Publish to ASF Nexus staging

Store the four credentials in `~/.gradle/gradle.properties` so they are not
exposed in your shell history:

```properties
mavenUsername=<your-asf-id>
mavenPassword=<your-asf-nexus-token>
signing.key=<ascii-armored-pgp-key>
signing.password=<key-passphrase>
```

Then run the publish task:

```bash
./gradlew :sdk:publish
```

Alternatively, pass them on the command line (note the single quotes around
properties whose values contain newlines or special characters):

```bash
./gradlew :sdk:publish \
  -PmavenUsername=<your-asf-id> \
  -PmavenPassword=<your-asf-nexus-token> \
  -P'signing.key=<ascii-armored-pgp-key>' \
  -P'signing.password=<key-passphrase>'
```

### Release

The process from now on should be the same as releasing other Airflow components.

### Dry-run against a local repository

To test the full publish flow without touching ASF infrastructure, override the
repository URL to a local directory (no signing key required since nothing goes
to Maven Central):

```bash
./gradlew :sdk:publish -PmavenUrl=file:///tmp/local-maven-repo
ls /tmp/local-maven-repo/org/apache/airflow/airflow-sdk/
```

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
