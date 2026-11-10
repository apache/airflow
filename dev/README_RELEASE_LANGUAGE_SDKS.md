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
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of contents**

- [The language SDKs](#the-language-sdks)
- [Verify the release candidate by PMC members](#verify-the-release-candidate-by-pmc-members)
- [Verify the release candidate by Contributors](#verify-the-release-candidate-by-contributors)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

This document describes how to verify a release candidate of the Airflow
language SDKs. How to prepare and publish each release is described in the
SDK's own `DEVELOPMENT.md`:

* [Java SDK](../java-sdk/DEVELOPMENT.md#publishing)
* [Go SDK](../go-sdk/DEVELOPMENT.md#calling-a-release-vote)
* [TypeScript SDK](../ts-sdk/DEVELOPMENT.md#publishing)

## The language SDKs

Each SDK is voted on as a signed source package in `dist/dev`. The commands
below use these values; substitute the row for the SDK being verified.

| SDK        | `<SDK>`    | `<TAG>`                    | Convenience binary         |
|------------|------------|----------------------------|----------------------------|
| Java       | `java-sdk` | `java-sdk/<VERSION>-rc<N>` | Maven, ASF Nexus staging   |
| Go         | `go-sdk`   | `go-sdk/v<VERSION>-rc<N>`  | none (module from the tag) |
| TypeScript | `ts-sdk`   | `ts-sdk/<VERSION>-rc<N>`   | npm, `rc` dist-tag         |

The source package is
`https://dist.apache.org/repos/dist/dev/airflow/<SDK>/<VERSION>-rc<N>/apache-airflow-<SDK>-<VERSION>-src.tar.gz`,
with its `.asc` signature and `.sha512` checksum next to it.

## Verify the release candidate by PMC members

Anyone on `dev@airflow.apache.org` can (and should) independently verify a
candidate before voting. Run these checks against the source package in
`dist/dev`; the release manager runs them too before sending the vote.

### Checksum

Confirm the published SHA-512 matches the downloaded tarball:

```bash
sha512sum -c apache-airflow-<SDK>-<VERSION>-src.tar.gz.sha512
```

### Signature

Import the `KEYS` file and verify the GPG signature:

```bash
curl -O https://downloads.apache.org/airflow/KEYS
gpg --import KEYS
gpg --verify apache-airflow-<SDK>-<VERSION>-src.tar.gz.asc \
    apache-airflow-<SDK>-<VERSION>-src.tar.gz
```

### Diff against the git tag

Extract the tarball and compare it with a clean checkout of the tag it claims
to be built from. The extracted top-level directory is
`apache-airflow-<SDK>-<VERSION>`, without the `-src` suffix that only appears
in the tarball's own filename:

```bash
tar xzf apache-airflow-<SDK>-<VERSION>-src.tar.gz
git clone --branch <TAG> https://github.com/apache/airflow.git tag-checkout
diff -rq apache-airflow-<SDK>-<VERSION>/ tag-checkout/<SDK>/
```

They should be identical except for files kept out of the source release via
`.gitattributes` `export-ignore`, and these expected differences:

* **Java:** the Gradle wrapper scripts and jar (`gradlew`, `gradlew.bat`,
  `gradle-wrapper.jar`) and `scripts` are omitted, since ASF source releases
  must not contain compiled code (see [LEGAL-570]). Filter them out with
  `| grep -vE ': (gradlew|gradlew\.bat|gradle-wrapper\.jar|scripts)$'`.
* **TypeScript:** the source package may be built from the commit before the
  tag, where `package.json` has the plain `<VERSION>`; the tag commit only
  changes it to `<VERSION>-rc<N>` for npm. The vote email names the commit when
  this applies, and the `version` line in `package.json` is then the only
  difference.

Any other diff output is unexpected and should block the vote.

[LEGAL-570]: https://issues.apache.org/jira/browse/LEGAL-570

### No binary files

ASF source releases must not contain compiled code. Scan for anything that
isn't text:

```bash
find apache-airflow-<SDK>-<VERSION>/ -type f \
  -exec sh -c 'file -b "$1" | grep -qviE "text|json|xml|empty" && echo "$1"' _ {} \;
```

This should print nothing.

### Build from source

**Java.** Regenerate the Gradle wrapper from a locally installed Gradle, then
build:

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

**Go.**

```bash
cd apache-airflow-go-sdk-<VERSION>
go build ./...
go test ./...
```

**TypeScript.**

```bash
cd apache-airflow-ts-sdk-<VERSION>
pnpm install --frozen-lockfile
pnpm test
pnpm run build
pnpm run verify:package
```

## Verify the release candidate by Contributors

Contributors can check that the release candidate works by using it from a
throwaway project.

### Java SDK

Resolve the staged Nexus artifacts to confirm they're actually consumable,
following the same pattern as the "Dry-run against a local repository" step in
[`java-sdk/DEVELOPMENT.md`](../java-sdk/DEVELOPMENT.md): point a
`repositories {}` block at the staging repository URL from the vote email,
declare a dependency on `org.apache.airflow:airflow-sdk-bom:<VERSION>`, and
confirm the transitive artifacts (including `airflow-sdk-jpl`) resolve and the
example bundle builds against them.

### Go SDK

The module is fetched straight from the tag:

```bash
go get github.com/apache/airflow/go-sdk@v<VERSION>-rc<N>
```

### TypeScript SDK

The release candidate is published on npm under the `rc` dist-tag:

```bash
npm install @apache-airflow/ts-sdk@<VERSION>-rc<N>
```
