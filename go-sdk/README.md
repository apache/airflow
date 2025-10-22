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

# Apache Airflow Go Task SDK

The Go SDK uses the Task Execution Interface (TEI or Task API) introduced in AIP-72 with Airflow 3.0.0 to give
Task functions written in Go full access to the Airflow "model", natively in go.

The Task API however does not provide a means to get the `ExecuteTaskWorkload` to the go worker itself. For
that we use the Edge Executor API.
Longer term we will likely need to stabilize the Edge Executor API and add versioning to it.

Since Go is a compiled language (putting aside projects such as [YAEGI](https://github.com/traefik/yaegi) that allow Go to be interpreted), all tasks must:

1. Be compiled into a binary ahead of time, and
2. Be registered inside the worker process in order to be executed.


> [!NOTE]
> This Golang SDK is under active development and is not ready for prime-time yet.

## Quickstart

- See [`example/bundle/main.go`](./example/bundle/main.go) for an example dag bundle where you can define your task functions

- Compile this into a binary:

  ```bash
  go build -o ./bin/sample-dag-bundle ./example/bundle
  ```

  (or see the [`Justfile`](./example/bundle/Justfile) for how you can build it and specify they bundle version number at build time.)

- Configure the go edge worker, by editing `$AIRFLOW_HOME/go-sdk.yaml`:

  These config values need tweaking, especially the ports and secrets. The ports are the default assuming
  airflow is running locally via `airflow standalone`.

  ```toml
  [edge]
  api_url = "http://0.0.0.0:8080/"

  [execution]
  api_url = "http://0.0.0.0:8080/execution"

  [api_auth]
  # This needs to match the value from the same setting in your API server for Edge API to function
  secret_key = "hPDU4Yi/wf5COaWiqeI3g=="

  [bundles]
  # Which folder to look in for pre-compiled bundle binaries
  folder = "./bin"

  [logging]
  # Where to write task logs to
  base_log_folder = "./logs"
  # Secret key matching airflow API server config, to only allow log requests from there.
  secret_key = "u0ZDb2ccINAbhzNmvYzclw=="
  ```

  You can also set these options via environment variables of `AIRFLOW__${section}_${key}`, for example `AIRFLOW__API_AUTH__SECRET_KEY`.

- Install the worker

  ```bash
  go install github.com/apache/airflow/go-sdk/cmd/airflow-go-edge-worker@latest
  ```

- Run it!

  ```bash
  airflow-go-edge-worker run --queues golang
  ```

### Example Dag:

You will need to create a python Dag and deploy it in to the Airflow

```python
from airflow.sdk import dag, task


@task.stub(queue="golang")
def extract(): ...


@task.stub(queue="golang")
def transform(): ...


@dag()
def simple_dag():

    extract() >> transform()


multi_language()
```

Here we see the `@task.stub` which tells the Dag parser about the "shape" of the go tasks, and lets us define
the relationships between them

> [!NOTE]
> Yes, you still have to have a python Dag file for now. This is a known limitation at the moment.

## Known missing features

A non-exhaustive list of features we have yet to implement

- Support for putting tasks into state other than success or failed/up-for-retry (deferred, failed-without-retries etc.)
- Remote task logs (i.e. S3/GCS etc)
- XCom reading/writing from other XCom backends


## Future Direction

This is more of an "it would be nice to have" than any plan or commitment, and a place to record ideas.

- The ability to run Airflow tasks "in" an existing code base - i.e. being able to define an Airflow task function that runs (in a goroutine) inside an existing code base an app.
- Do the task function reflection ahead of time, not for each Execute call.
