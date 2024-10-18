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
  - [ ] Handle SIG-INT/CTRL+C and gracefully terminate and complete job
  - [ ] Add a stop command
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

### Test on Windows

Create wheel on Linux:

``` bash
breeze release-management generate-constraints --python 3.10
breeze release-management prepare-provider-packages --package-format wheel --include-removed-providers remote
breeze release-management prepare-airflow-package
```

Copy the files to Windows

On Windows "cheat sheet", Assume Python 3.10 installed, files mounted in Z:\Temp:

``` text
python -m venv airflow-venv
airflow-venv\Scripts\activate.bat

pip install --constraint Z:\temp\constraints-source-providers-3.10.txt Z:\temp\apache_airflow_providers_remote-0.1.0-py3-none-any.whl Z:\temp\apache_airflow-2.10.0.dev0-py3-none-any.whl

set AIRFLOW_ENABLE_AIP_44=true
set AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION=True
set AIRFLOW__CORE__INTERNAL_API_URL=http://nas:8080/remote_worker/v1/rpcapi
set AIRFLOW__SCHEDULER__SCHEDULE_AFTER_TASK_EXECUTION=False
set AIRFLOW__CORE__EXECUTOR=RemoteExecutor
set AIRFLOW__CORE__DAGS_FOLDER=dags
set AIRFLOW__LOGGING__BASE_LOG_FOLDER=logs

airflow remote worker --concurrency 4 --queues windows
```

Notes on Windows:

- PR https://github.com/apache/airflow/pull/40424 fixes PythonOperator
- Log folder temple must replace run_id colons _or_ DAG must be triggered with Run ID w/o colons as not allowed as file name in Windows
