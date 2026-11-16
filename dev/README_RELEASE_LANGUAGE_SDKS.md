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
- [Check the release candidate commit](#check-the-release-candidate-commit)
- [After a successful vote](#after-a-successful-vote)
- [If the vote fails](#if-the-vote-fails)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

This document describes the release steps shared by the Airflow language SDKs:
verifying a release candidate, and what to do after the vote. How to build,
stage and call the vote for each release, and the SDK-specific publishing
steps, are described in the SDK's own `DEVELOPMENT.md`:

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
shasum -a 512 -c apache-airflow-<SDK>-<VERSION>-src.tar.gz.sha512
```

`shasum` ships with both macOS and most Linux distributions; `sha512sum -c`
works too on Linux but is not installed on macOS by default.

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

If the vote email says the source package was built from a commit before the
tag, diff the tarball against that commit instead, then confirm the tag adds
nothing to the SDK directory except the version bump. Limit the diff to the
SDK directory; the tag usually sits on a release branch, so an unscoped diff
also lists unrelated commits that landed in between:

```bash
git -C tag-checkout diff <SOURCE-COMMIT> <TAG> --stat -- <SDK>/
```

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

Run `gradle wrapper` with that same Gradle version. The task configures
`buildSrc`, and a different Gradle resolves a `kotlin-dsl` plugin version that
`verification-metadata.xml` does not pin, so strict verification fails before
the wrapper is written. If your installed Gradle differs, download the pinned
distribution, check it against the same SHA-256, and use it directly:

```bash
curl -LO <GRADLE-DISTRIBUTION-URL>
echo "<GRADLE-DISTRIBUTION-SHA>  gradle-<GRADLE-VERSION>-bin.zip" | shasum -a 256 -c
unzip -q gradle-<GRADLE-VERSION>-bin.zip
cd apache-airflow-java-sdk-<VERSION>
../gradle-<GRADLE-VERSION>/bin/gradle build
```

That Gradle version must also support the JDK it runs on; for example,
Gradle 8.14 cannot run on JDK 26.

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

## Check the release candidate commit

The RC tag must point at a commit that is on `main` or on the core release
branch the SDK is released from (`vX-Y-test` or `vX-Y-stable`). Core release
branches are occasionally rewritten. A rewrite after the RC is tagged leaves
the voted commit reachable only through the tag, and the TypeScript release
workflow then refuses to publish from it.

Run this just before sending the vote email, and again before tagging the final
release:

```bash
git fetch upstream
git merge-base --is-ancestor <rc-commit> upstream/<release-branch> \
  && echo "OK: on <release-branch>" \
  || echo "NOT on <release-branch>"
```

If the check fails before the vote, find the rewritten branch's equivalent
commit (`git rev-parse <old-commit>:<SDK>` and `git rev-parse <new-commit>:<SDK>`
print the same tree), tag the RC there, and name that commit in the vote email.

## After a successful vote

Reply to the vote thread with a `[RESULT][VOTE]` email once the vote has been
open for at least 72 hours and has at least three binding +1 votes and more
binding +1 than -1 votes. List the binding and non-binding votes separately.
Then follow the steps below. The final tag and the binary publishing differ per
SDK:

| SDK        | `<SDK-NAME>`     | `<FINAL-TAG>`        | Publish binaries                                                                          |
|------------|------------------|----------------------|-------------------------------------------------------------------------------------------|
| Java       | `Java SDK`       | `java-sdk/<VERSION>` | [Release the Nexus staging repository](../java-sdk/DEVELOPMENT.md#after-a-successful-vote) |
| Go         | `Go SDK`         | `go-sdk/v<VERSION>`  | [Warm the Go module proxy](../go-sdk/DEVELOPMENT.md#after-a-successful-vote)              |
| TypeScript | `TypeScript SDK` | `ts-sdk/<VERSION>`   | [Publish to npm](../ts-sdk/DEVELOPMENT.md#after-a-successful-vote)                        |

### Move the source package to `dist/release`

```bash
svn mv https://dist.apache.org/repos/dist/dev/airflow/<SDK>/<VERSION>-rc<N> \
       https://dist.apache.org/repos/dist/release/airflow/<SDK>/<VERSION> \
       -m "Release Apache Airflow <SDK-NAME> <VERSION>"
```

Then remove the release this one supersedes. Per the
[ASF release distribution policy](https://infra.apache.org/release-distribution.html#archival),
`dist/release` should hold the latest release of each line still under
development, and older ones remain available from `archive.apache.org`.
Confirm the previous release is on the archive before removing it:

```bash
curl -fsI https://archive.apache.org/dist/airflow/<SDK>/<PREVIOUS-VERSION>/apache-airflow-<SDK>-<PREVIOUS-VERSION>-src.tar.gz
svn rm https://dist.apache.org/repos/dist/release/airflow/<SDK>/<PREVIOUS-VERSION> \
       -m "Remove superseded Apache Airflow <SDK-NAME> <PREVIOUS-VERSION>"
```

### Tag the final version

Run the [commit check](#check-the-release-candidate-commit) again, then tag the
voted commit and push the tag:

```bash
git tag -s <FINAL-TAG> <voted-commit> -m "Apache Airflow <SDK-NAME> <VERSION>"
git push upstream <FINAL-TAG>
```

Keep the RC tag for traceability.

For TypeScript, the voted commit is the one the source package was built from,
where `package.json` has the plain `<VERSION>`. The vote email names it. The RC
tag's own commit has `<VERSION>-rc<N>` in `package.json`, so the release
workflow cannot publish the final version from it.

The "Protect language SDK release tags" ruleset in `.asf.yaml` blocks deleting
or moving SDK tags for everyone except committers. Get the tag right the first
time anyway: once the Go module proxy has fetched a tag, it keeps that commit
forever.

### Publish the binaries

Follow the SDK's own steps linked in the table above.

### Publish a GitHub release

Attach the **voted, signed** source artifacts, exported from `dist/release`
and verified again:

```bash
svn export https://dist.apache.org/repos/dist/release/airflow/<SDK>/<VERSION>
cd <VERSION>
shasum -a 512 -c apache-airflow-<SDK>-<VERSION>-src.tar.gz.sha512
gpg --verify apache-airflow-<SDK>-<VERSION>-src.tar.gz.asc \
    apache-airflow-<SDK>-<VERSION>-src.tar.gz

gh release create <FINAL-TAG> \
  --repo apache/airflow \
  --title "Apache Airflow <SDK-NAME> <VERSION>" \
  --notes "<the Changes since line from the vote email>" \
  --verify-tag \
  --latest=false \
  --prerelease \
  apache-airflow-<SDK>-<VERSION>-src.tar.gz \
  apache-airflow-<SDK>-<VERSION>-src.tar.gz.asc \
  apache-airflow-<SDK>-<VERSION>-src.tar.gz.sha512
```

Drop the `--prerelease` flag if this is not a prerelease. Keep
`--latest=false` either way. Otherwise the SDK release replaces Airflow core
as the repository's *Latest* release.

### Publish the API docs

**Java and TypeScript.** Trigger the *Publish Docs to S3* workflow for the
final tag:

```bash
gh workflow run "Publish Docs to S3" --repo apache/airflow --ref main \
  -f ref=<FINAL-TAG> \
  -f include-docs=<SDK> \
  -f destination=live
```

Optionally use `destination=staging` first to check, then `live`. Each run
spends about 15 minutes building the CI image before the SDK jobs start. When
releasing Java and TypeScript together, one run can build both with
`-f include-docs="java-sdk ts-sdk"`, from any ref where both SDK directories
match their voted sources.

Confirm that `https://airflow.apache.org/docs/<SDK>/<VERSION>/` resolves.
`https://airflow.apache.org/docs/<SDK>/stable/` can show the previous version
in the CDN for a while after publishing.

**Go.** The API docs are on pkg.go.dev, which the Go SDK's publishing step
covers.

### Announce

Wait until the binaries are available and the docs URL resolves:

* **Java:** Maven Central has synced (the BOM resolves for a fresh consumer).
  This takes about an hour after the release.
* **Go:** pkg.go.dev shows the version.
* **TypeScript:** `npm view @apache-airflow/ts-sdk dist-tags.latest` prints
  `<VERSION>`.

Then send a plain-text `[ANNOUNCE]` email to `users@airflow.apache.org`, cc
`dev@airflow.apache.org`:

```text
Subject: [ANNOUNCE] Apache Airflow <SDK-NAME> <VERSION> Released

Dear Airflow community,

I'm happy to announce that Apache Airflow <SDK-NAME> <VERSION> was just released.

The signed source release can be downloaded from:
https://downloads.apache.org/airflow/<SDK>/<VERSION>/

<BINARIES>

The API documentation is available at:
<DOCS-URL>

<Optional: one or two lines on notable changes; omit for the first release.>

Thanks to everyone who contributed to and tested this release.

Cheers,
<your name>
```

Fill `<BINARIES>` and `<DOCS-URL>` for the SDK:

* **Java.**
  `<BINARIES>`: "We also published the convenience binaries to Maven Central,
  under group id org.apache.airflow. Import airflow-sdk-bom:<VERSION> to keep
  the module versions aligned."
  `<DOCS-URL>`: `https://airflow.apache.org/docs/java-sdk/<VERSION>/`
* **Go.**
  `<BINARIES>`: "Add it to a Go project with:
  go get github.com/apache/airflow/go-sdk@v<VERSION>"
  `<DOCS-URL>`: `https://pkg.go.dev/github.com/apache/airflow/go-sdk@v<VERSION>`
* **TypeScript.**
  `<BINARIES>`: "We also published the convenience package to npm as
  @apache-airflow/ts-sdk. Install it with:
  npm install @apache-airflow/ts-sdk@<VERSION>"
  `<DOCS-URL>`: `https://airflow.apache.org/docs/ts-sdk/<VERSION>/`

Run `grep '<' email.txt` on the rendered email and confirm **no output**. Any
match means a placeholder was left unfilled.

### Record the release

Add the release (version and date) in the ASF Committee Report Helper:
<https://reporter.apache.org/addrelease.html?airflow>. Name it
`<SDK-NAME> <VERSION>`, for example `Java SDK 1.0.0`, to match earlier entries
such as `Java SDK 1.0.0-beta1` and `Task SDK x.y.z`.

## If the vote fails

Reply to the vote thread to close it, then:

1. Remove the candidate from `dist/dev`:

   ```bash
   svn rm https://dist.apache.org/repos/dist/dev/airflow/<SDK>/<VERSION>-rc<N> \
          -m "Remove failed Apache Airflow <SDK-NAME> <VERSION>-rc<N>"
   ```

2. Undo the SDK-specific publishing, as described under "If the vote fails" in
   the SDK's `DEVELOPMENT.md`.
3. Fix the issue and cut the next RC (`<VERSION>-rc<N+1>`). The released version
   stays the same; only the RC counter in the tag increments. Keep the failed
   RC tag.
