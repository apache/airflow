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

These sequence diagrams explain the lifecycle of a Job with relation to the database
operation in the context of the internal API of Airflow.

As part of AIP-44 implementation we separated the ORM Job instance from the code the job runs,
introducing a concept of Job Runners. The Job Runner is a class that is responsible for running
the code and it might execute either in-process when direct database is used, or remotely when
the job is run remotely and communicates via internal API (this part is a work-in-progress and we
will keep on updating these lifecycle diagrams).

This apply to all of the CLI components Airflow runs (Scheduler, DagFileProcessor, Triggerer,
Worker) that run a job. The AIP-44 implementation is not yet complete, but when complete it will
apply to some of the components (DagFileProcessor, Triggerer, Worker) and not to others (Scheduler).

## In-Process Job Runner

```mermaid
sequenceDiagram
    participant CLI component
    participant JobRunner
    participant DB

    activate CLI component
    CLI component-->>DB: Create Session

    activate DB

    CLI component->>DB: Create Job
    DB->>CLI component: Job object

    CLI component->>JobRunner: Create Job Runner
    JobRunner ->> CLI component: JobRunner object

    CLI component->>JobRunner: Run Job

    activate JobRunner

    JobRunner->>DB: prepare_for_execution [Job]
    DB->>JobRunner: prepared

    par
        JobRunner->>JobRunner: execute_job
    and
        JobRunner->>DB: access DB (Variables/Connections etc.)
        DB ->> JobRunner: returned data
    and
        JobRunner-->>DB: create heartbeat session
        Note over DB: Note: During heartbeat<br> two DB sessions <br>are opened in parallel(!)
        JobRunner->>DB: perform_heartbeat [Job]
        JobRunner ->> JobRunner: Heartbeat Callback [Job]
        DB ->> JobRunner: heartbeat response
        DB -->> JobRunner: close heartbeat session
    end

    JobRunner->>DB: complete_execution [Job]
    DB ->> JobRunner: completed

    JobRunner ->> CLI component: completed
    deactivate JobRunner

    deactivate DB
    deactivate CLI component
```

## Internal API Job Runner (WIP)

```mermaid
sequenceDiagram
    participant CLI component
    participant JobRunner
    participant Internal API
    participant DB

    activate CLI component


    CLI component->>Internal API: Create Job
    Internal API-->>DB: Create Session
    activate DB
    Internal API ->> DB: Create Job
    DB ->> Internal API: Job object
    DB --> Internal API: Close Session
    deactivate DB

    Internal API->>CLI component: JobPydantic object

    CLI component->>JobRunner: Create Job Runner
    JobRunner ->> CLI component: JobRunner object

    CLI component->>JobRunner: Run Job

    activate JobRunner

    JobRunner->>Internal API: prepare_for_execution [JobPydantic]

    Internal API-->>DB: Create Session
    activate DB
    Internal API ->> DB: prepare_for_execution [Job]
    DB->>Internal API: prepared
    DB-->>Internal API: Close Session
    deactivate DB
    Internal API->>JobRunner: prepared

    par
        JobRunner->>JobRunner: execute_job
    and
        JobRunner ->> Internal API: access DB (Variables/Connections etc.)
        Internal API-->>DB: Create Session
        activate DB
        Internal API ->> DB: access DB (Variables/Connections etc.)
        DB ->> Internal API: returned data
        DB-->>Internal API: Close Session
        deactivate DB
        Internal API ->> JobRunner: returned data
    and
        JobRunner->>Internal API: perform_heartbeat <br> [Job Pydantic]
        Internal API-->>DB: Create Session
        activate DB
        Internal API->>DB: perform_heartbeat [Job]
        Internal API ->> Internal API: Heartbeat Callback [Job]
        DB ->> Internal API: heartbeat response
        Internal API ->> JobRunner: heartbeat response
        DB-->>Internal API: Close Session
        deactivate DB
    end

    JobRunner->>Internal API: complete_execution  <br> [Job Pydantic]
    Internal API-->>DB: Create Session
    Internal API->>DB: complete_execution [Job]
    activate DB
    DB ->> Internal API: completed
    DB-->>Internal API: Close Session
    deactivate DB
    Internal API->>JobRunner: completed
    JobRunner ->> CLI component: completed

    deactivate JobRunner

    deactivate CLI component
```
