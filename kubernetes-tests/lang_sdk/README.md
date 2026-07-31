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

# lang-SDK coordinator system test (KubernetesExecutor)

End-to-end test that one Dag mixing **Python + Go + Java** tasks runs to success on
`KubernetesExecutor`, using the per-queue `extra.pod_template_file` routing added to the
`[sdk] coordinators` config. The test lives at
`kubernetes-tests/tests/kubernetes_tests/test_lang_sdk_coordinator_executor.py`.

## How it fits together

```
                    localstack (S3)                     scheduler (KubernetesExecutor)
   go-artifacts ─┐   ┌ dags bucket ── S3DagBundle ──► dag-processor parses lang_sdk_combined.py
   java-artifacts┘   │                                         │ task on queue golang/java
                     │                                         ▼
   stub Dag ─────────┘                       reads [sdk] coordinators[key].extra.pod_template_file
                                                              │
                          worker pod (from that pod template):
                          initContainer  stage_artifacts.py  ── S3DagBundle.initialize() ──►
                              pulls go-artifacts / java-artifacts bucket into the shared
                              emptyDir = executables_root / jars_root
                          base container  supervisor → coordinator forks the Go binary / Java jar
```

Key point: in coordinator mode the worker pod does **not** run the Python task-runner,
so the artifact is not downloaded by the normal Dag-bundle path. The init container runs
`stage_artifacts.py`, which reuses the **DagBundle interface**
(`DagBundlesManager().get_bundle(name).initialize()` — the download half of
`task_runner.parse`) to pull the artifact from its S3 bucket into the path the
coordinator scans.

## Components

| Path | Role |
| --- | --- |
| `dags/lang_sdk_combined.py` | Python stub Dag (`dag_id=lang_sdk_combined`); uploaded to the `dags` bucket. |
| `go_example/` | Go bundle sources (own module, `replace` onto `../../../go-sdk`): `go_extract` / `go_transform` under `lang_sdk_combined`. |
| `java_example/` | Java bundle sources (standalone Gradle build, SDK from mavenLocal): `java_extract` / `java_transform` under `lang_sdk_combined`. |
| `stage_artifacts.py` | Init-container entrypoint; stages an artifact bucket via DagBundle. |
| `pod_templates/lang_sdk_golang.yaml` | `golang` queue worker pod: prod image + go-artifacts init container. |
| `pod_templates/lang_sdk_java.yaml` | `java` queue worker pod: JVM image + java-artifacts init container. |
| `manifests/localstack.yaml` | In-cluster S3 (localstack). |
| `config/values.yaml` | Helm overrides: KubernetesExecutor, coordinators (+extra.pod_template_file), queue routing, stub-Dag S3 bundle, AWS conn, scheduler pod-template mount. |

The Go binary, Java jar, and stub Dag share one object store (localstack) but live in
**separate buckets** (`go-artifacts`, `java-artifacts`, `dags`).

## SDK sources always come from upstream main

`go-sdk/` and `java-sdk/` are young, fast-moving directories that a release/backport branch may lack
entirely or carry a stale, branch-cut-frozen copy of. So regardless of which branch/ref this repo is
checked out at, `breeze k8s setup-lang-sdk-test` (and `run-complete-tests --lang-sdk-test`) always
builds the Go bundle and Java jar from upstream `main`'s `go-sdk`/`java-sdk` — fetched fresh via
`_lang_sdk_fetch_upstream_sdk_sources()` in `kubernetes_commands.py`, never from whatever's on disk.
Everything else — `airflow-core/`, `task-sdk/`, the deployed Airflow image, and this directory's own
`go_example`/`java_example` harness fixtures — still comes from the checked-out branch as before, so a
backport of a core/task-sdk fix to a release-test branch keeps testing against current SDK code.

## Running it

The artifacts, localstack, config, and Helm release are provisioned by a single breeze
command on top of an already-deployed KubernetesExecutor cluster:

```bash
# 1. Stand up a KubernetesExecutor cluster.
#    * configure-cluster creates the `airflow` namespace and test resources.
#    * --rebuild-base-image bakes the local (unreleased) cncf.kubernetes executor
#      and task-SDK coordinator code into the image -- without it the released
#      providers ship instead and the coordinator routing is ignored.
#    * ui compile-assets must run first so the rebuilt prod image ships the UI
#      assets the api-server health endpoint serves (the shared test harness
#      polls it before running).
breeze k8s create-cluster
breeze k8s configure-cluster
breeze ui compile-assets
breeze k8s build-k8s-image --rebuild-base-image
breeze k8s upload-k8s-image
breeze k8s deploy-airflow --executor KubernetesExecutor

# 2. Provision the lang-SDK test: build the Go bundle + Java jar (in Docker),
#    build + load the Java worker image (prod + JRE for the JavaCoordinator),
#    deploy localstack, upload artifacts + stub Dag, render config, helm upgrade.
breeze k8s setup-lang-sdk-test

# 3. Run the test by name (the shared harness triggers a fresh Dag run). The test is gated on
#    RUN_LANG_SDK_K8S_TESTS so it stays out of the regular k8s suites; set it to run the test here.
RUN_LANG_SDK_K8S_TESTS=true breeze k8s tests --executor KubernetesExecutor \
    -- -k test_lang_sdk_combined_dag_succeeds
```

In CI (and for a one-shot local run) steps 2-3 are folded into a single `run-complete-tests` call via
`breeze k8s run-complete-tests --lang-sdk-test`: it provisions the lang-SDK env after the base deploy,
then runs the test. Rather than bolting this onto the regular k8s system-test matrix (which ran it
redundantly on all six `KubernetesExecutor` / standard-naming-off jobs and added ~6 minutes each), the
`k8s-tests.yml` workflow runs it in a **dedicated `tests-kubernetes-lang-sdk` job** on a single default
Python-Kubernetes combo (the `lang-sdk-kubernetes-combo` input, wired from the `default-python-version`
and `default-kubernetes-version` build-info outputs). That job sets `RUN_LANG_SDK_K8S_TESTS=true`
(which `--lang-sdk-test` reads) and runs only the lang-SDK test (`-k
test_lang_sdk_combined_dag_succeeds`), not the full suite; the regular system-test matrix no longer runs
it at all. The provisioning builds (Go bundle, Java jar, Java worker image) and the localstack deploy
run in parallel.

By default the Go bundle and Java jar are built inside ephemeral toolchain containers so a dev host
needs neither Go nor a JDK installed. In CI the dedicated job sets `LANG_SDK_NATIVE_TOOLCHAIN=true`,
which makes breeze build both artifacts with the host `go` / `./gradlew` instead: the workflow installs
the toolchains via `actions/setup-go` and `actions/setup-java` and restores the Go module/build cache
and the Gradle distribution + dependency cache with `actions/cache`, so the build skips the per-run
toolchain-image pulls and cold dependency downloads. The cache keys carry a `-v1-` salt and a
`runner.arch` segment (see `lang-sdk-go-v1-` / `lang-sdk-gradle-v1-` in `k8s-tests.yml`) — bump the salt
to force-invalidate a poisoned cache; the arch segment keeps the amd64 and arm64 caches separate. The
JDK version comes from the `java-sdk-version` build-info output (the `JAVA_SDK_VERSION` breeze
constant).
