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

# Implementation Status

https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-69+Remote+Executor

## Primary Functionality - MVP

- [x] Model -> DB Table creation
- [ ] Breeze
  - [x] Support RemoteExecutor
  - [ ] Start Remote Worker panel
  - [x] Hatch dynamically load plugin(s)
- [x] Bootstrap Provider Package
- [ ] Executor class
  - [x] Writes new jobs
  - [ ] Acknowledge success/fail
  - [ ] Can terminate a job
  - [ ] Archiving of job table
- [ ] Plugin
  - [x] REST API
  - [ ] REST API Authentication
  - [ ] Allow starting REST API separate
  - [x] Expose jobs via UI
  - [ ] Expose active remote worker
- [ ] Remote Worker
  - [x] CLI
  - [x] Get a job and execute it
  - [x] Report result
  - [x] Heartbeat
  - [x] Queues
  - [x] Retry on connection loss
  - [ ] Send logs
  - [ ] Archive logs on completions
  - [ ] Can terminate job
  - [ ] Check version match
  - [?] Handle SIG-INT/CTRL+C and gracefully terminate and complete job
- [ ] Web UI
  - [ ] Show logs while executing
  - [ ] Show logs after completion
- [ ] Configurability
- [ ] Documentation
  - [ ] References in Airflow core
  - [ ] Provider Package docs
- [ ] Tests
- [ ] AIP-69
  - [x] Draft
  - [x] Update specs
  - [ ] Vote

## Future Feature Collection

- [ ] Support for API token on top of normal auth
- [ ] API token per worker
- [ ] Plugin
  - [ ] Overview about queues
  - [ ] Administrative maintenance
- [ ] Remote Worker
  - [ ] Multiple jobs / concurrency
  - [ ] Publish system metrics with heartbeats
  - [ ] Remote update
  - [ ] Integration into telemetry to send metrics
- [ ] API token provisioning can be automated
- [ ] Move task context generation from Remote to Executor ("Need to know", depends on Task Execution API)
- [ ] Thin deployment
- [ ] Test/Support on Windows
- [ ] DAG Code push (no need to GIT Sync)
- [ ] Scaling test

## Notes

```
/opt/airflow/airflow/providers/remote/_start_internal_api.sh
/opt/airflow/airflow/providers/remote/_start_task.sh
/opt/airflow/airflow/providers/remote/_start_celery_worker.sh
```
