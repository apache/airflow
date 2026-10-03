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

End-to-end tests of the lang-SDK coordinators on `KubernetesExecutor`, using the per-queue
`extra.pod_template_file` routing added to the `[sdk] coordinators` config: one Dag mixing
**Python + Go + Java** tasks runs to success, and a Python task on a routed queue that has no artifact
fails with its reason. The tests live at
`kubernetes-tests/tests/kubernetes_tests/test_lang_sdk_coordinator_executor.py`.

## How it fits together

```
   localstack (S3)
   dags ───────────── S3DagBundle lang-sdk-dags ───────┐
   java-artifacts ─── S3DagBundle java-task-handlers ──┤
   go-artifacts ───── stage_artifacts.py ► emptyDir ───┤
                                                       ▼
                dag-processor parses lang_sdk_combined.py, checks its stub tasks against
                the Go binary / Java jar and binds each stub task to the artifact that
                registers its handler
                                                       │
                scheduler (KubernetesExecutor): a task on queue golang/java is sent with its
                bound artifact, and the pod comes from
                [sdk] coordinators[key].extra.pod_template_file
                                                       │
                task pod (from that pod template):
                  golang: initContainer stage_artifacts.py ── S3DagBundle.initialize() ──►
                          pulls the go-artifacts bucket into the shared emptyDir =
                          go-task-handlers LocalDagBundle
                  java:   the base container downloads the java-task-handlers S3DagBundle
                  base container  supervisor → coordinator forks the bound Go binary / Java jar
```

Key point: the Java task handler bundle is the `java-artifacts` bucket itself. `java-task-handlers`
is an `S3DagBundle` in `dagProcessor.dagBundleConfigList`: the dag-processor refreshes it, and the
Java task pod downloads it when the task starts, with the connection from
`AIRFLOW_CONN_AWS_LOCALSTACK` on its `base` container. The Go task handler bundle cannot be read
that way, because an S3 download drops the execute bit that the coordinator requires. So
`go-task-handlers` names a `LocalDagBundle` over a shared `emptyDir`, and an init container fills it
by running `stage_artifacts.py`. It reuses the **DagBundle interface**
(`DagBundlesManager().get_bundle(name).initialize()`, the download half of `task_runner.parse`) to
pull the binary from its S3 bucket, then restores its execute bit.

The misrouted Dag (`lang_sdk_misrouted.py`) has a plain Python task on the `golang` queue. The
dag-processor binds no artifact to it, the scheduler still queues it, and the worker fails it
because its Dag file is not an artifact that the Go coordinator runs. The test reads the reason from
the task's state reason. The Go pod gets the S3 connection for it: a task without an artifact reads
its own Dag file from its Dag bundle, which is the S3 bundle of the stub Dags.

The dag-processor pod stages the Go bucket the same way (`dagProcessor.extraInitContainers`) and
reads the Java bucket as an S3 bundle, because it runs the Go binary and the Java jar to check the
stub tasks of `lang_sdk_combined.py` against the task handlers they register. A stub task without a
handler fails the Dag file's import. Running `setup-lang-sdk-test` again uploads new artifacts: the
dag-processor reads a new Java jar when it refreshes its bundles, and restages the Go binary only
when its pod restarts. The dag-processor needs a JRE for the jar, and the chart sets one image for
every Airflow component, so `setup-lang-sdk-test` runs the Airflow components on the Java worker
image. The Java coordinator's `extra` takes its image from the same chart value, so `--java-image`
also reaches the Java task pods. The other task pods keep the plain prod image: the setup pins
`[kubernetes_executor] worker_container_repository` and `worker_container_tag` to it.

## Components

| Path | Role |
| --- | --- |
| `dags/lang_sdk_combined.py` | Python stub Dag (`dag_id=lang_sdk_combined`); uploaded to the `dags` bucket. |
| `dags/lang_sdk_misrouted.py` | Python task on the `golang` queue with no artifact (`dag_id=lang_sdk_misrouted`); uploaded to the `dags` bucket. |
| `go_example/` | Go bundle sources (own module, `replace` onto `../../../go-sdk`): `go_extract` / `go_transform` under `lang_sdk_combined`. |
| `java_example/` | Java bundle sources (standalone Gradle build, SDK from mavenLocal): `java_extract` / `java_transform` under `lang_sdk_combined`. |
| `stage_artifacts.py` | Init-container entrypoint; stages the Go artifact bucket via DagBundle and restores the execute bit. |
| `pod_templates/lang_sdk_golang.yaml` | `golang` queue worker pod: prod image + go-artifacts init container. |
| `pod_templates/lang_sdk_java.yaml` | `java` queue worker pod: JVM image; the coordinator downloads the java-artifacts S3 bundle. |
| `manifests/localstack.yaml` | In-cluster S3 (localstack). |
| `config/values.yaml` | Helm overrides: KubernetesExecutor, coordinators (+extra.pod_template_file), queue routing, stub-Dag S3 bundle, artifact Dag bundles, dag-processor Go init container, AWS conn, scheduler pod-template mount. |

The Go binary, Java jar, and Dag files share one object store (localstack) but live in
**separate buckets** (`go-artifacts`, `java-artifacts`, `dags`), and `java-artifacts` is itself the
Java task handler bundle.

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
built from a later `main`: the Dag processor cannot probe the Go bundle, and the import error of the
stub Dag says `Version '<date>' not found in supervisor schema bundle`. Building the checkout's own
SDK is also what makes the k8s test exercise a PR's SDK changes: `go_example`/`java_example` are
harness fixtures that track the checked-out branch, so compiling them against a *different* SDK
means any SDK rename in the PR fails to build.

The upstream-`main` fallback is only for a branch cut before `go-sdk`/`java-sdk` existed. When it
kicks in, that copy and the branch's `go_example` can diverge (upstream may change go-sdk's
dependency graph while `go_example`'s committed `go.sum` is tidied against the in-repo go-sdk), so
the Go bundle build re-runs `go mod tidy` in its scratch workspace before packing and reconciles to
whichever `go-sdk` it is compiled against. The committed `go_example` `go.sum` is untouched and
stays guarded by the `check-go-example-mod-tidy` prek hook.

Everything else — `airflow-core/`, `task-sdk/`, the deployed Airflow image, and this directory's own
`go_example`/`java_example` fixtures — always comes from the checked-out branch.

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
#    deploy localstack, upload artifacts + stub Dags, render config, helm upgrade
#    with the Airflow components on the Java worker image.
breeze k8s setup-lang-sdk-test

# 3. Run the tests by class name (the shared harness triggers a fresh Dag run for each). The tests are
#    gated on RUN_LANG_SDK_K8S_TESTS so they stay out of the regular k8s suites; set it to run them here.
RUN_LANG_SDK_K8S_TESTS=true breeze k8s tests --executor KubernetesExecutor \
    -- -k TestLangSdkCoordinatorExecutor
```

In CI (and for a one-shot local run) steps 2-3 are folded into a single `run-complete-tests` call via
`breeze k8s run-complete-tests --lang-sdk-test`: it provisions the lang-SDK env after the base deploy,
then runs the tests. Rather than bolting this onto the regular k8s system-test matrix (which ran it
redundantly on all six `KubernetesExecutor` / standard-naming-off jobs and added ~6 minutes each), the
`k8s-tests.yml` workflow runs it in a **dedicated `tests-kubernetes-lang-sdk` job** on a single default
Python-Kubernetes combo (the `lang-sdk-kubernetes-combo` input, wired from the `default-python-version`
and `default-kubernetes-version` build-info outputs). That job sets `RUN_LANG_SDK_K8S_TESTS=true`
(which `--lang-sdk-test` reads) and runs only the lang-SDK tests (`-k
TestLangSdkCoordinatorExecutor`), not the full suite; the regular system-test matrix no longer runs
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
