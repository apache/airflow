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

# 🚧 Apache Airflow Go Task SDK 🚧

> [!NOTE]
> This Golang SDK is under active development and is not ready for prime-time yet.

This README is primarily aimed at developers working _on_ the Go-SDK itself. Users wishing to write Airflow tasks in Go should look at the reference docs, but those don't exist yet.

## How It Works

The Go SDK uses the Task Execution Interface (TEI or Task API) introduced in AIP-72 with Airflow 3.0.0.

The Task API however does not provide a means to get the `ExecuteTaskWorkload` to the go worker itself. For the short term, we make use of [gopher-celery](github.com/marselester/gopher-celery) to get tasks from a Redis broker. Longer term we will likely need to stabilize the Edge Executor API and write a go client for that.

Since Go is a compiled language (putting aside projects such as [YAEGI](https://github.com/traefik/yaegi) that allow go to be interpreted) all tasks must be a) compiled in to the binary, and b) "registered" inside the worker process in order to be executed.

## Current state

This SDK currently will:

- Get tasks from Celery queue(s)
- Run registered tasks (no support for dag versioning or loading of multiple different "bundles")
- Heartbeat and report the final state of the final TI
- Allow access to Variables

## Known missing features

A non-exhaustive list of features we have yet to implement

- Reading of Airflow Connections
- Support for putting tasks into state other than success or failed/up-for-retry (deferred, failed-without-retries etc.)
- HTTP Log server to view logs from in-progress tasks
- Remote task logs (i.e. S3/GCS etc)
- XCom reading/writing from API server
- XCom reading/writing from other XCom backends


## Future Direction

This is more of an "it would be nice to have" than any plan or commitment, and a place to record ideas.

- Support multiple versions by compiling tasks/bundles into plugins and make use of [go-plugin](https://github.com/hashicorp/go-plugin) (This is how Terraform providers work)

  This would enable use to have executor code and task code in separate processes, and to be able to have a single worker execute different bundles/versions of tasks (i.e. we'd have a go executor process that launches versioned plugin bundles to actually execute the task)
