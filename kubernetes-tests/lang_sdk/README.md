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

# lang-SDK coordinator system tests (KubernetesExecutor)

Two end-to-end tests exercise every lang-SDK coordinator (Go, Java, TypeScript) on
`KubernetesExecutor`:

- **`test_lang_sdk_mixed_language.py`** runs one Dag mixing **Python + Go + Java + TypeScript**
  tasks, using the `extra.pod_template_file` routing in the `[sdk] coordinators` config.
- **`test_lang_sdk_native_dag.py`** runs one Dag **declared entirely in each language SDK** (no
  Python file at all): the Dag processor asks the packed Java jar / TypeScript bundle to parse
  itself. Go has no native Dag parsing support yet, so its class is unconditionally skipped.

Every coordinator is configured once, with no `[sdk] dag_bundle_to_coordinator` entry needed:
with exactly one coordinator per language, there is nothing to disambiguate.

## How it fits together

```
                    localstack (S3)                        scheduler (KubernetesExecutor)
   go-task-handlers ─┐  ┌ dags bucket ── S3DagBundle ──► dag-processor parses lang_sdk_mixed_language.py
   java-task-handlers┤  │                                         │ task on queue golang/java/typescript
   ts-task-handlers ─┘  │                                         ▼
   (each + .airflowignore)                       reads [sdk] coordinators[key].extra.pod_template_file
   stub Dag ────────────┘                                         │
                                              worker pod (shared lang_sdk_worker.yaml template):
                                              base container  supervisor → coordinator downloads its
                                              own task_handler_bundle_name bundle (S3DagBundle.initialize(),
                                              then marks the matched file executable) → forks the
                                              Go binary / Java jar / TypeScript bundle

   lang-sdk-native-java ── S3DagBundle ──► dag-processor parses the jar  (java-sdk coordinator)
   lang-sdk-native-ts   ── S3DagBundle ──► dag-processor parses the bundle (ts-sdk coordinator)
```

Key points:

- There is no init container and no staging step: a coordinator downloads its own
  `task_handler_bundle_name` (or, for a native task, the task's own Dag bundle) directly, the same
  S3DagBundle machinery any Python Dag bundle uses. The Go binary loses its execute bit through the
  S3 download; the coordinator restores it on the matched file once its footer and `binary_sha256`
  verify, so no side-channel staging is needed for that either.
- `go-task-handlers`, `java-task-handlers` and `ts-task-handlers` each carry an `.airflowignore`
  that excludes everything in them, so the Dag processor never tries to parse the packed Go bundle
  or the Java/TypeScript handler-only bundles as Dags. A coordinator's own artifact lookup does not
  read `.airflowignore`, so stub tasks still find their handlers there.
- `lang-sdk-native-java` and `lang-sdk-native-ts` carry no such file: parsing them is the point.

## Components

| Path | Role |
| --- | --- |
| `dags/lang_sdk_mixed_language.py` | Python stub Dag (`dag_id=lang_sdk_mixed_language`); uploaded to the `dags` bucket. |
| `go_example/` | Go bundle sources (own module, `replace` onto `../../../go-sdk`): `go_extract` / `go_transform` under `lang_sdk_mixed_language`. |
| `java_example/` | Java bundle sources (standalone Gradle build, SDK from mavenLocal): `java_extract` / `java_transform` under `lang_sdk_mixed_language`. |
| `ts_example/` | TypeScript bundle sources (standalone pnpm project, SDK via a `file:` dependency): `ts_extract` / `ts_transform` under `lang_sdk_mixed_language`. |
| `airflowignore_all` | Uploaded as `.airflowignore` to every task-handler bucket. |
| `pod_templates/lang_sdk_worker.yaml` | Worker pod template shared by every coordinator queue. |
| `Dockerfile.runtimes` | Prod image plus a headless JRE and Node.js, which every coordinator (and the Dag processor) needs. |
| `manifests/localstack.yaml` | In-cluster S3 (localstack). |
| `config/values.yaml` | Helm overrides: KubernetesExecutor, coordinators (+extra.pod_template_file), queue routing, every Dag bundle, AWS conn, scheduler pod-template mount. |

The Go binary, Java jar, TypeScript bundle and the Dag files share one object store (localstack)
but live in **separate buckets** (`go-task-handlers`, `java-task-handlers`, `ts-task-handlers`,
`dags`, `lang-sdk-native-java`, `lang-sdk-native-ts`).

The native TypeScript bundle is `ts-sdk/example`, packed by `airflow-ts-pack`. It carries both
halves of that example: the handlers for the Python-declared `typescript_example` Dag, and the
natively declared `typescript_native_example`. `typescript_example.py` is uploaded to the `dags`
bucket too, because the native Dag's trigger task starts a run of it and waits for that run,
deferring to `DagStateTrigger` in the Python triggerer. The test un-pauses `typescript_example`
for that reason. The native Java Dag reuses `airflow-e2e-tests/java-native-bundle`, the same fixture
the compose e2e builds.

## Which SDK sources get built

`breeze k8s setup-lang-sdk-test` (and `run-complete-tests --lang-sdk-test`) resolves them in
`_lang_sdk_resolve_sdk_sources()` in `kubernetes_commands.py`:

| Checkout | Go/Java SDK sources |
| --- | --- |
| has `go-sdk/` and `java-sdk/` | its own copies |
| has neither (or only one) | upstream `main`, fetched fresh via `_lang_sdk_fetch_upstream_sdk_sources()` |

The checkout's own copies win because they are the ones that pair with the Airflow this test
deploys. A packed bundle declares a dated `supervisor_schema_version` and the task-SDK supervisor
rejects a bundle whose version it does not know, so a release branch's Airflow cannot run an SDK
built from a later `main` — the Go task fails with `cannot find executable bundle with usable
supervisor_schema_version`. Building the checkout's own SDK is also what makes the k8s test exercise
a PR's SDK changes: `go_example`/`java_example` are harness fixtures that track the checked-out
branch, so compiling them against a *different* SDK means any SDK rename in the PR fails to build.
The in-repo `ts-sdk` is always used (TypeScript has no upstream-main fallback path).

The upstream-`main` fallback is only for a branch cut before `go-sdk`/`java-sdk` existed. When it
kicks in, that copy and the branch's `go_example` can diverge (upstream may change go-sdk's
dependency graph while `go_example`'s committed `go.sum` is tidied against the in-repo go-sdk), so
the Go bundle build re-runs `go mod tidy` in its scratch workspace before packing and reconciles to
whichever `go-sdk` it is compiled against. The committed `go_example` `go.sum` is untouched and
stays guarded by the `check-go-example-mod-tidy` prek hook.

Everything else — `airflow-core/`, `task-sdk/`, the deployed Airflow image, and this directory's own
`go_example`/`java_example`/`ts_example` fixtures — always comes from the checked-out branch.

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

# 2. Provision the lang-SDK test: build the Go bundle, the Java jars and the
#    TypeScript bundles (in Docker), build + load the shared runtime image
#    (prod + JRE + Node, which every coordinator and the Dag processor use),
#    deploy localstack, upload artifacts + Dag files, render config, helm upgrade.
breeze k8s setup-lang-sdk-test

# 3. Run the tests by name (the shared harness triggers a fresh Dag run). They are gated on
#    RUN_LANG_SDK_K8S_TESTS so they stay out of the regular k8s suites; set it to run them here.
RUN_LANG_SDK_K8S_TESTS=true breeze k8s tests --executor KubernetesExecutor -- -k TestLangSdk
```

In CI (and for a one-shot local run) steps 2-3 are folded into a single `run-complete-tests` call via
`breeze k8s run-complete-tests --lang-sdk-test`: it provisions the lang-SDK env after the base deploy,
then runs the tests. Rather than bolting this onto the regular k8s system-test matrix (which ran it
redundantly on all six `KubernetesExecutor` / standard-naming-off jobs and added several minutes each),
the `k8s-tests.yml` workflow runs it in a **dedicated `tests-kubernetes-lang-sdk` job** on a single
default Python-Kubernetes combo (the `lang-sdk-kubernetes-combo` input, wired from the
`default-python-version` and `default-kubernetes-version` build-info outputs). That job sets
`RUN_LANG_SDK_K8S_TESTS=true` (which `--lang-sdk-test` reads) and runs only `-k TestLangSdk`, not the
full suite; the regular system-test matrix no longer runs it at all. The provisioning builds (Go
bundle, Java jars, TypeScript bundles) and the localstack deploy run in parallel.

By default the Go bundle, Java jars and TypeScript bundles are built inside ephemeral toolchain
containers so a dev host needs neither Go, a JDK, nor Node installed. In CI the dedicated job sets
`LANG_SDK_NATIVE_TOOLCHAIN=true`, which makes breeze build every artifact with the host `go` /
`./gradlew` / `pnpm` instead: the workflow installs the toolchains via `actions/setup-go`,
`actions/setup-java` and `actions/setup-node`, and restores the Go module/build cache, the Gradle
distribution + dependency cache, and the pnpm store with `actions/cache`, so the build skips the
per-run toolchain-image pulls and cold dependency downloads. The cache keys carry a `-v1-` salt and
a `runner.arch` segment (see `lang-sdk-go-v1-` / `lang-sdk-gradle-v1-` / `lang-sdk-pnpm-v1-` in
`k8s-tests.yml`) — bump the salt to force-invalidate a poisoned cache; the arch segment keeps the
amd64 and arm64 caches separate. The JDK version comes from the `java-sdk-version` build-info
output (the `JAVA_SDK_VERSION` breeze constant).
