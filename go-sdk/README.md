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

# Airflow Go SDK

Write Apache Airflow Dags and tasks in Go.

Requires Go 1.25 or later.

## Installation

```bash
go get github.com/apache/airflow/go-sdk
go get -tool github.com/apache/airflow/go-sdk/cmd/airflow-go-pack
```

## Quick start

Declare a Dag in `main.go`:

```go
package main

import (
    "log"

    "github.com/apache/airflow/go-sdk/airflow"
)

func extract(airflow.Context) (int, error) {
    return 42, nil
}

func report(actx airflow.Context, rows int) error {
    actx.Logger().InfoContext(actx, "extracted rows", "rows", rows)
    return nil
}

func main() {
    dag := airflow.Dag("go_hello", airflow.DagSpec{Schedule: "@daily", Queue: "golang"})

    extracted := dag.Task(extract)
    dag.Task(report, airflow.Inputs(extracted))

    bundle := airflow.Bundle()
    bundle.Register(dag)
    if err := bundle.Serve(); err != nil {
        log.Fatal(err)
    }
}
```

Build it into a bundle, a single executable:

```bash
go tool airflow-go-pack --output dist/go_hello .
```

Put `dist/go_hello` in a Dag bundle, such as the default `dags-folder` bundle, and send the `golang` queue to the
Go coordinator in `airflow.cfg`:

```ini
[sdk]
coordinators = {"go": {"classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator"}}
queue_to_coordinator = {"golang": "go"}
```

Airflow reads the bundle like any other Dag file: `go_hello` shows up in the UI, runs on its schedule, and
`report` receives the value `extract` returned.

## Learn more

The [Go SDK guide][guide] covers:

| Topic                                                                                         | Guide section                                  |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| Task inputs, options, order-only dependencies, task groups, branching, triggering another Dag | [Declaring a Dag in Go][declaring-a-dag-in-go] |
| The `airflow.Context`, the `sdk.Client` and the task state store                              | [Writing tasks][writing-tasks]                 |
| Building a bundle with `airflow-go-pack`, and cross-platform builds                           | [Building a bundle][building-a-bundle]         |
| Where to deploy a bundle, and the `ExecutableCoordinator` options                             | [Deploying][deploying]                         |
| What a Dag declared in Go cannot do yet                                                       | [Limitations][limitations]                     |

See [`example/`](./example) for working projects.

[guide]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/go.html
[declaring-a-dag-in-go]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/go.html#declaring-a-dag-in-go
[writing-tasks]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/go.html#writing-tasks
[building-a-bundle]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/go.html#building-a-bundle
[deploying]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/go.html#deploying
[limitations]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/go.html#limitations

## Compatibility matrix

Which Airflow TaskInstance states and capabilities this SDK supports. This table is generated from
[`capabilities.yaml`](capabilities.yaml); the conformance dimensions are defined in the
[Language SDK conformance spec](https://github.com/apache/airflow/blob/main/contributing-docs/30_new_language_sdk.rst).
The minimum version applies to the current SDK source, while "Since" records when each capability
first became available and can therefore be earlier than the current minimum.
Do not edit the table by hand — edit `capabilities.yaml` and let the `update-go-sdk-readme-matrix`
prek hook regenerate it.

<!-- BEGIN AUTO-GENERATED LANG-SDK COMPAT MATRIX -->

*Min. Airflow version: 3.4 · supervisor schema: 2026-10-30*

| Dimension | Tier | Supported | Since | Notes |
|---|---|---|---|---|
| **TaskInstance states** |  |  |  |  |
| state: `success` | MUST | ✓ | 3.3 |  |
| state: `failed` | MUST | ✓ | 3.3 |  |
| state: `up_for_retry` | MUST | ✓ | 3.3 | RetryTask |
| state: `skipped` | SHOULD | ✓ | 3.4 | airflow.TriggerDagRun with SkipWhenAlreadyExists only |
| state: `deferred` | MAY | ✓ | 3.4 | airflow.TriggerDagRun with Deferrable only; resumes in the Go runtime |
| state: `up_for_reschedule` | MAY | ✗ | – | runtime does not emit RescheduleTask yet |
| state: `awaiting_input` | MAY | ✗ | – | runtime does not emit AwaitInputTask yet |
| state: `removed` | MAY | ✓ | 3.3 |  |
| **Runtime capabilities** |  |  |  |  |
| capability: `mixed-lang-stub-target` | MUST | ✓ | 3.3 | @task.stub |
| capability: `taskflow-binding` | MUST | ✗ | – | bind @task.stub literal/XCom args to the native handler |
| capability: `task-logging` | MUST | ✓ | 3.3 | slog records streamed over the logs socket |
| capability: `xcom-read-write` | MUST | ✓ | 3.3 | PushXCom / GetXCom |
| capability: `connection-read` | MUST | ✓ | 3.3 | GetConnection |
| capability: `variable-read-write` | MUST | ✓ | 3.4 |  |
| capability: `self-contained-bundle` | MUST | ✓ | 3.3 | AFBNDL01 native binary via airflow-go-pack |
| capability: `retry-policy` | MAY | ✗ | – | no task-facing retry-policy API yet |
| capability: `task-state-store` | MAY | ✓ | 3.4 |  |
| capability: `asset-state-store` | MAY | ✗ | – | no task-facing state-store API yet |
| capability: `asset-event-emit` | MAY | ✗ | – | runtime does not emit asset events yet |
| capability: `asset-event-read` | MAY | ✗ | – | no task-facing asset-event API yet |
| **Native-Dag authoring** |  |  |  |  |
| capability: `native-dag-authoring` | SHOULD | ✗ | – | native Dag authoring not implemented yet |
| capability: `task-args` | MUST † | n/a | – |  |
| capability: `dag-params` | MUST † | n/a | – |  |
| capability: `taskflow-dependencies` | MUST † | n/a | – |  |
| capability: `branching` | SHOULD † | n/a | – |  |
| capability: `dag-test` | SHOULD † | n/a | – |  |
| capability: `task-group` | MAY † | n/a | – |  |
| capability: `dynamic-task-mapping` | MAY † | n/a | – |  |
| capability: `asset-inlets-outlets` | MAY † | n/a | – |  |
| capability: `asset-scheduling` | MAY † | n/a | – |  |
| capability: `object-store` | MAY † | n/a | – | no object-storage API yet |

*Marks: ✓ supported · ✗ not supported · n/a not applicable. A tier marked † applies only when `native-dag-authoring` is supported.*

<!-- END AUTO-GENERATED LANG-SDK COMPAT MATRIX -->

## Links

- [Go SDK guide][guide]
- [API reference](https://pkg.go.dev/github.com/apache/airflow/go-sdk) on pkg.go.dev
- [Source](https://github.com/apache/airflow/tree/main/go-sdk): the `go-sdk/` directory of the Apache Airflow monorepo
- [Issues](https://github.com/apache/airflow/issues): bug reports and feature requests
- [Developing this module](./DEVELOPMENT.md): building, testing, code generation, publishing and
  design decisions
- [Releasing the language SDKs](../dev/README_RELEASE_LANG_SDKS.md): the release cycle and branch
  every language SDK release is cut from
