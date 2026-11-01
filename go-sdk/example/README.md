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

# Go SDK examples

Example Dags for the Apache Airflow Go SDK. See the [Go SDK guide][guide] for the full documentation.

[guide]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/go.html

## Build

Run these from `go-sdk/`. Each command writes one bundle into `bin/`:

```bash
# Dag definition
go tool airflow-go-pack --output bin/go_native ./example/native

go build -o bin/dynamic/go_dynamic_dags.raw ./example/dynamic
cp example/dynamic/dags.yaml bin/dynamic/dags.yaml
go tool airflow-go-pack --executable bin/dynamic/go_dynamic_dags.raw \
  --source example/dynamic/main.go --output bin/go_dynamic_dags

# Python Dag with stub TaskHandler
go tool airflow-go-pack --output bin/example_dags ./example/bundle
```

The dynamic example reads `dags.yaml` next to its binary, including when `airflow-go-pack` reads its Dags, so it
is built first and packed in place.

## Configure Airflow

Lay out the bundles like this:

```text
<dags_folder>/
  go/go_native
  go-dynamic/go_dynamic_dags
  go-dynamic/dags.yaml
  go_examples.py              # copied from go-sdk/dags/
/opt/airflow/go-task-handlers/
  example_dags
  .airflowignore              # containing "*"
```

Register `go-task-handlers` as a Dag bundle, and route the `golang` queue to the Go coordinator:

```bash
export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
  {"name": "dags-folder", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {}},
  {
    "name": "go-task-handlers",
    "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
    "kwargs": {"path": "/opt/airflow/go-task-handlers"}
  }
]'
export AIRFLOW__SDK__COORDINATORS='{
  "go": {
    "classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator",
    "kwargs": {"task_handler_bundle_name": "go-task-handlers"}
  }
}'
export AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{"golang": "go"}'
```

The bundles must be built for the platform of the workers and the Dag processor.

The examples read this Connection:

```bash
airflow connections add test_http --conn-type http --conn-host example.com
```

## Dag definition

These Dags are declared entirely in Go:

| Dag                                            | Source                     | Shows                                                                                  |
| ---------------------------------------------- | -------------------------- | -------------------------------------------------------------------------------------- |
| `go_native_pipeline`                           | `native/main.go`           | Task inputs, literals, a task group, an edge label, `If`, `Switch`, and a trigger rule |
| `go_native_trigger`                            | `native/main.go`           | `TriggerDagRun`, which starts `go_native_report`                                       |
| `go_native_report`                             | `native/report.go`         | A Dag declared in its own file, which the Code view shows                              |
| `go_dynamic_a`, `go_dynamic_b`, `go_dynamic_c` | `dynamic/` and `dags.yaml` | One Dag per entry of `dags.yaml`, which changes the Dags with no rebuild               |

`go_native_pipeline` reads two Variables. Set `go_native_cadence` to `weekly` to take the other branch of
`Switch`:

```bash
airflow variables set go_native_greeting "hello from Airflow"
airflow variables set go_native_cadence daily

airflow dags trigger go_native_pipeline
airflow dags trigger go_native_trigger
```

## Python Dag with stub TaskHandler

The Python Dags in `go-sdk/dags/go_examples.py` declare stub tasks, and `bundle/` implements them:

| Dag                    | Shows                                                                          |
| ---------------------- | ------------------------------------------------------------------------------ |
| `simple_dag`           | Python and Go tasks in one Dag, TaskFlow arguments, Connections, and a retry   |
| `taskflow_binding_dag` | Each way a stub task's arguments bind to a Go handler: by position and by name |
| `concurrent_xcom_dag`  | Reading XComs from goroutines                                                  |
| `variable_write_dag`   | Writing and deleting Variables                                                 |
| `task_state_dag`       | The task state store                                                           |

`simple_dag` reads one Variable:

```bash
airflow variables set my_variable "hello from Airflow"

airflow dags trigger simple_dag
```
