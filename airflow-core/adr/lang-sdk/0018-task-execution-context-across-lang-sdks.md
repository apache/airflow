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

# ADR-0018: Task Execution Context Across Lang SDKs

## Status

Proposed.

## Context

The Python Task SDK context holds the same value in many places: `context["run_id"]`,
`context["dag_run"].run_id`, `context["ti"].run_id` and `context["ti"].dag_run.run_id`. Each is
mutable, so they can disagree. The Lang SDKs are drifting the same way, and since they are still
experimental, fixing them now is cheap. The Python Task SDK is out of scope.

## Decision

Lang SDKs expose the task execution context as one read-only `ti` object with a nested `dagRun`, and
store every value once.

```
ti
  taskId               string
  mapIndex             int         -1 when the task is not mapped
  tryNumber            int
  dagRun
    dagId              string
    runId              string
    runType            enum
    runAfter           timestamp
    logicalDate        timestamp   nullable
    dataIntervalStart  timestamp   nullable
    dataIntervalEnd    timestamp   nullable
    conf               map         empty when the run has no conf
```

The context is a container. Each SDK names keys in its own language's convention.

An SDK may also expose `dagRun` at the top level, pointing at `ti.dagRun`, for convenience. This is
optional. An SDK must not add it if its context mapping is mutable.

Future ADRs may add fields for new SDK features. A new field must still be stored only once; any
duplication must be raised explicitly in that ADR and agreed on.

## Consequences

The Go, Java and TypeScript SDKs each need a breaking change to match this shape. They make it
without a deprecation period.
