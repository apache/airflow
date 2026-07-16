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

See also Airflow documentation on Java SDK under *Authoring and Scheduling* for
more details on using the Java SDK.

The SDK requires Java 11 or later at runtime. Optional components and
development tools may have further requirements (see the toolchain in
`buildSrc/src/main/kotlin/airflow-jvm-conventions.gradle.kts`).

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

Any versions published to Maven Central (instead of Snapshots) are considered
releases, including alpha, beta, etc (see the
[ASF Release Policy](https://www.apache.org/legal/release-policy.html#release-types)).
Every release therefore requires a PMC vote before it is published; only
`-SNAPSHOT` builds may be published without a vote.

### Prerequisites

* An ASF committer account with access to
  [repository.apache.org](https://repository.apache.org).
* A GPG key that has been
  [added to the project KEYS file](https://infra.apache.org/release-signing.html)
  and uploaded to a public key server.

### Bump the version

Edit `gradle.properties` and set the version for this release:

```properties
projectVersion=<VERSION>
```

Commit the change and push it to the release branch.

Use a Maven-compatible version string, as defined by the
[Maven version order specification]. For example, version 1 beta 1 is
`1.0.0-beta1`, and the eventual version 1 release is `1.0.0`.

[Maven version order specification]: https://maven.apache.org/pom.html#Version_Order_Specification

*NOTE:* Editing `gradle.properties` as above is the standard procedure. You can
alternatively override the version for a single command without editing the
file, by passing `-PprojectVersion=<VERSION>` to any Gradle invocation. This
is handy for one-off or pre-release builds. Either way, `main` should stay on a
`-SNAPSHOT` version between releases: a snapshot sorts after `<VERSION>` and
before the GA, so no extra bump is needed after a beta.

### Tag the release candidate

Tag the release commit, keeping the RC number in the tag name so a failed vote
simply bumps to the next RC (the artifact version itself does not carry the RC
suffix). Push the tag before sending the vote so reviewers can check out the
exact source being voted on.

```bash
git tag -s java-sdk/<VERSION>-rc<N> -m "Java SDK <VERSION> RC <N>"
git push upstream java-sdk/<VERSION>-rc<N>
```

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

Then stage and close the release. The
[Gradle Nexus Publish Plugin](https://github.com/gradle-nexus/publish-plugin)
(applied in the root `build.gradle.kts`) gathers every module into one staging
repository, so there is no per-module fan-out to reconcile.

```bash
./gradlew publishToApache closeApacheStagingRepository \
  --no-configuration-cache \
  -P"signing.key=$(gpg --armor --export-secret-keys your-gpg-key-fingerprint)"
```

*NOTE:* The signing key is supplied through the command line since it contains
newlines, which does not work well in a Gradle properties file.

*NOTE:* You can also use the following environment variables to provide the
credentials instead: `ASF_NEXUS_USERNAME`, `ASF_NEXUS_PASSWORD`, `SIGNING_KEY`,
and `SIGNING_PASSWORD`. This is especially useful on e.g. CI.

*NOTE:* We enable Gradle's configuration cache globally, but the staging tasks
(`publishToApache`, `closeApacheStagingRepository`, `releaseApacheStagingRepository`)
talk to the Nexus REST API and are not configuration-cache compatible. Hence
the `--no-configuration-cache` flag on the release commands.

### Verify the upload

Under *Staging Repositories* on the
[ASF Nexus server](https://repository.apache.org/), open the closed repository
and verify it contains all modules, each with its jar, `-sources.jar`,
`-javadoc.jar` (where applicable), `.pom`, and `.asc` signature.

Check *Updated by* (should be your ID), *Uploaded Date*, and *Last Modified*.

### Upload the source package

The closed staging repository from the previous step is the convenience-binary
URL you link in the vote.

The signed source package is the artifact the vote is formally on; the Maven
artifacts are convenience binaries. The `sourceRelease` task builds it from the
committed `java-sdk` sources (`LICENSE` and `NOTICE` included) and produces its
signature and checksum in one step:

```bash
# Signing uses your local gpg keyring, so have your key/passphrase ready.
./gradlew sourceRelease -PgitRef=java-sdk/<VERSION>-rc<N>
```

This writes three files to `build/distributions/`:

```
apache-airflow-java-sdk-<VERSION>-src.tar.gz
apache-airflow-java-sdk-<VERSION>-src.tar.gz.asc
apache-airflow-java-sdk-<VERSION>-src.tar.gz.sha512
```

**NOTE:** The source archive omits the Gradle wrapper scripts (`gradlew`,
`gradlew.bat`) and `gradle/wrapper/gradle-wrapper.jar` since ASF source releases
must not contain compiled code (see [LEGAL-570]), and the scripts are not useful
without the jar. The `gradle/wrapper/gradle-wrapper.properties` file is kept so
the archive pins the Gradle version and distribution checksum for verification
when the wrapper is regenerated.

[LEGAL-570]: https://issues.apache.org/jira/browse/LEGAL-570

Copy the three files into your checkout of the ASF dist *dev* repo and commit
them with Subversion. If you don't already have the repo checked out, replace
`<dist-dev-checkout>` with wherever you want it and `<path-to>` with this
project's location:

```bash
# One-time: check out the Airflow dist dev area.
svn checkout https://dist.apache.org/repos/dist/dev/airflow <dist-dev-checkout>

cd <dist-dev-checkout>
mkdir -p java-sdk/<VERSION>-rc<N>
cp <path-to>/java-sdk/build/distributions/apache-airflow-java-sdk-<VERSION>-src.tar.gz* \
   java-sdk/<VERSION>-rc<N>/

svn add --parents java-sdk/<VERSION>-rc<N>
svn commit -m "Add Apache Airflow Java SDK <VERSION>-rc<N> source release candidate"
```

The commit publishes them under
`https://dist.apache.org/repos/dist/dev/airflow/java-sdk/<VERSION>-rc<N>/`, which
is the source-package URL you link in the vote.

### Call the vote

Send a `[VOTE]` email to `dev@airflow.apache.org` linking the git tag and
commit, the source package in `dist/dev`, the closed Nexus staging repository,
and the `KEYS` file. See "Vote email template" below for the exact fields to
fill in, and "Verifying a release" for what to ask reviewers to check.

### Vote email template

```text
Subject: [VOTE] Release Apache Airflow Java SDK <VERSION> based on <VERSION>-rc<N>

Hi,

I would like to call a vote to release Apache Airflow Java SDK <VERSION>, based
on release candidate <VERSION>-rc<N>.

Changes since <the last release>: <a one-line description>

The release candidate contains the following Maven artifacts, all under group id org.apache.airflow:

- Main SDK API
    - airflow-sdk
    - airflow-sdk-processor
- Logger helpers
    - airflow-sdk-jpl
    - airflow-sdk-jul
    - airflow-sdk-log4j2
    - airflow-sdk-slf4j
- Gradle plugin and the marker artifact
    - airflow-sdk-gradle-plugin
    - org.apache.airflow.sdk.gradle.plugin
- BOM
    - airflow-sdk-bom

Git information:

- Tag: java-sdk/<VERSION>-rc<N>
- Commit: <full-commit-sha>
- https://github.com/apache/airflow/releases/tag/java-sdk%2F<VERSION>-rc<N>

Source release, signatures and checksums:
https://dist.apache.org/repos/dist/dev/airflow/java-sdk/<VERSION>-rc<N>/

Convenience binaries (staged in the ASF Nexus repository):
https://repository.apache.org/content/repositories/orgapacheairflow-<NNNN>/

KEYS file (public keys used to sign the release):
https://downloads.apache.org/airflow/KEYS

Please review and vote. The vote will remain open for at least 72 hours, until <YYYY-MM-DD HH:MM UTC>, or until the necessary number of binding votes is reached.

[ ] +1 Release this package as Apache Airflow Java SDK <VERSION>
[ ] +0 No opinion
[ ] -1 Do not release, because ...

Only votes from Airflow PMC members are binding, but everyone is welcome and encouraged to test the release and vote.

The verification process can be found in the main repository:
https://github.com/apache/airflow/tree/java-sdk/<VERSION>-rc<N>/java-sdk#verifying-a-release

For more details on ASF release verification, see: https://www.apache.org/info/verification.html

Best,
<your name>
```

Pre-send checklist:

* Every staged artifact above resolves in the Nexus staging repository. Cross
  check against the BOM, not just this list from memory.
* `Changes since rc<N-1>` and the vote deadline are filled in.
* Run `grep '<' email.txt` on the rendered email and confirm **no output** —
  any match means a template placeholder (`<N>`, `<NNNN>`, `<YYYY-MM-DD ...>`,
  etc.) was left unfilled.

### Verifying a release

Anyone on `dev@airflow.apache.org` can (and should) independently verify a
candidate before voting. Below is the checklist a reviewer — or the release
manager, before sending the vote — should run against the source package in
`dist/dev`.

1. **Checksum.** Confirm the published SHA-512 matches the downloaded tarball:

   ```bash
   sha512sum -c apache-airflow-java-sdk-<VERSION>-src.tar.gz.sha512
   ```

2. **Signature.** Import the `KEYS` file and verify the GPG signature:

   ```bash
   curl -O https://downloads.apache.org/airflow/KEYS
   gpg --import KEYS
   gpg --verify apache-airflow-java-sdk-<VERSION>-src.tar.gz.asc \
       apache-airflow-java-sdk-<VERSION>-src.tar.gz
   ```

3. **Diff against the git tag.** Extract the tarball and compare it with a
   clean checkout of the tag it claims to be built from. They should be
   identical except for the files kept out of the source release via
   `.gitattributes` `export-ignore`. The extracted top-level directory should be
   `apache-airflow-java-sdk-<version>` without the `-src` suffix that only
   appears in the tarball's own filename:

   ```bash
   tar xzf apache-airflow-java-sdk-<VERSION>-src.tar.gz
   git clone --branch java-sdk/<VERSION>-rc<N> \
     https://github.com/apache/airflow.git tag-checkout
   diff -rq apache-airflow-java-sdk-<VERSION>/ tag-checkout/java-sdk/ \
     | grep -vE ': (gradlew|gradlew\.bat|gradle-wrapper\.jar|scripts)$'
   ```

   Any remaining diff output is unexpected and should block the vote.

4. **No binary files.** ASF source releases must not contain compiled code.
   Scan for anything that isn't text:

   ```bash
   find apache-airflow-java-sdk-<VERSION>/ -type f \
     -exec sh -c 'file -b "$1" | grep -qviE "text|json|xml|empty" && echo "$1"' _ {} \;
   ```

   This should print nothing.

5. **Build from source.** Regenerate the Gradle wrapper from a locally installed
   Gradle (see the *Upload the source package* section above):

   ```bash
   cd apache-airflow-java-sdk-<VERSION>
   gradle wrapper \
       --gradle-distribution-url <GRADLE-DISTRIBUTION-URL> \
       --gradle-distribution-sha256-sum <GRADLE-DISTRIBUTION-SHA>
   ./gradlew build
   ```

   Use values from `distributionUrl` and `distributionSha256Sum` in the bundled
   `gradle/wrapper/gradle-wrapper.properties` to fill in
   `<GRADLE-DISTRIBUTION-URL>` and `<GRADLE-DISTRIBUTION-SHA>`.

6. **Staged-binary smoke test.** Resolve the staged Nexus artifacts from a
   throwaway project to confirm they're actually consumable, following the
   same pattern as the "Dry-run against a local repository" step: point a
   `repositories {}` block at the staging repository URL, declare a dependency
   on `org.apache.airflow:airflow-sdk-bom:<VERSION>`, and confirm the
   transitive artifacts (including `airflow-sdk-jpl`) resolve and the example
   bundle builds against them.

### After a successful vote

Reply with a `[RESULT][VOTE]` tally, then:

1. **Release** the staging repository so the artifacts sync to Maven Central
   (a few hours). Nothing is rebuilt or re-signed:

   ```bash
   ./gradlew releaseApacheStagingRepository --no-configuration-cache
   ```

   (Or click *Release* on the repository in the Nexus UI.)

2. **Move** the source package from `dist/dev` to `dist/release`:

   ```bash
   svn mv https://dist.apache.org/repos/dist/dev/airflow/java-sdk/<VERSION>-rc<N> \
          https://dist.apache.org/repos/dist/release/airflow/java-sdk/<VERSION> \
          -m "Release Apache Airflow Java SDK <VERSION>"
   ```

3. **Tag** the final version on the same commit that was voted:

   ```bash
   git tag -s java-sdk/<VERSION> <voted-commit-hash> -m "Apache Airflow Java SDK <VERSION>"
   git push upstream java-sdk/<VERSION>
   ```

   Keep the RC tag for traceability.

4. Publish a **GitHub release**. Attach the **voted, signed** source artifacts.
   From the directory holding the three signed files (e.g. `dist/release`):

   ```bash
   gh release create java-sdk/<VERSION> \
     --repo apache/airflow \
     --title "Apache Airflow Java SDK <VERSION>" \
     --notes "See the Java SDK README for the features in this release." \
     --verify-tag \
     --prerelease \
     apache-airflow-java-sdk-<VERSION>-src.tar.gz \
     apache-airflow-java-sdk-<VERSION>-src.tar.gz.asc \
     apache-airflow-java-sdk-<VERSION>-src.tar.gz.sha512
   ```

   Drop the `--prerelease` flag if this is not a prerelease.

5. **Announce.** Wait ~1 hour after promoting so Maven Central has synced, then
   send a plain-text `[ANNOUNCE]` email to `users@airflow.apache.org` (cc
   `dev@airflow.apache.org`) using the "Announce email template" below, and
   record the release (version + date) in the ASF Committee Report Helper:
   <https://reporter.apache.org/addrelease.html?airflow>.

6. Publish the **API docs.** Trigger the *Publish Docs to S3* workflow in
   `apache/airflow` for the release tag:

   ```bash
   gh workflow run "Publish Docs to S3" --repo apache/airflow --ref main \
     -f ref=java-sdk/<VERSION> \
     -f include-docs=java-sdk \
     -f destination=live
   ```

   Optionally use `destination=staging` first to check, then `live`. It may be
   possible to `auto` instead, which resolves to `live` for a release tag.

   Confirm that `https://airflow.apache.org/docs/java-sdk/stable/` resolves
   (allow time for cache invalidation) and `/docs/java-sdk/` redirects to it.

### Announce email template

Send to `users@airflow.apache.org`, cc `dev@airflow.apache.org`.

```text
Subject: [ANNOUNCE] Apache Airflow Java SDK <VERSION> Released

Dear Airflow community,

I'm happy to announce that Apache Airflow Java SDK <VERSION> was just released.

The signed source release can be downloaded from:
https://downloads.apache.org/airflow/java-sdk/<VERSION>/

We also published the convenience binaries to Maven Central, under group id
org.apache.airflow. Import airflow-sdk-bom:<VERSION> to keep the module versions
aligned.

The API documentation is available at:
https://airflow.apache.org/docs/java-sdk/<VERSION>/

<Optional: one or two lines on notable changes; omit for the first release.>

Thanks to everyone who contributed to and tested this release.

Cheers,
<your name>
```

Pre-send checklist:

* The Maven Central sync has completed (the BOM resolves for a fresh consumer)
  and the docs URL above resolves.
* Fill or delete the optional changes line.
* Run `grep '<' email.txt` on the rendered email and confirm **no output** --
  any match means a placeholder (`<VERSION>`, `<your name>`, ...) was left
  unfilled.


### If the vote fails

Close the vote, **drop** the staging repository in Nexus, remove the `dist/dev`
candidate, fix the issue, and cut the next RC (`...-rc2`). The released version
stays the same (e.g. `<VERSION>`); only the RC counter in the tag increments.

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
