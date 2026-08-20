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

# 6. Cross-language TaskFlow argument binding

Date: 2026-08-20

## Status

Accepted.

## Context

[Lang-SDK ADR 0007](../../airflow-core/adr/lang-sdk/0007-taskflow-across-language-boundary.md)
defines the language-neutral `arg_bindings` contract. This ADR records how the Go SDK consumes
that contract. A Go executable cannot reconstruct a Python operator, so every value needed by its
task function must arrive in `StartupDetails` or be resolved through the coordinator.

## Decision

The coordinator path consumes `arg_bindings` without introducing a Go-specific wire format:

```text
Python stub Dag
  transform("uk", extract())
            |
            v
Airflow core: Dag serialization
  ordered arg_bindings
  +-- literal(name=country, value="uk", schema=string)
  `-- xcom(name=extracted, task_id=extract, schema=object)
            |
            v
Serialized Dag -- ti_run --> StartupDetails.TIContext.ArgBindings
                                      |
                                      v
Go coordinator: convertArgBindings --> []binding.Arg
                                      |
                                      v
Task.Execute --> Plan.Resolve --> Go task function
```

The Go SDK analyzes the function signature during bundle registration, then stores the plan on the
task wrapper for that bundle process:

```text
reflect.Type
     |
     v
binding.Analyze
     |
     +-- injectable slots: context / TI context / logger / SDK client
     |
     `-- data slots
          +-- multiple or non-struct parameters -> flat positional plan
          `-- one struct parameter             -> sole-struct plan
     |
     v
stored Plan
```

At execution, literals stay inline while all upstream XCom values are pulled concurrently:

```text
ORDERED []binding.Arg

[0] literal(value) ---------------------------------------------> raws[0]

[1] xcom(extract) -- goroutine --> GetXCom(extract) --\
[2] xcom(config)  -- goroutine --> GetXCom(config)  ----+--> wait --> raws[1..3]
[3] xcom(model)   -- goroutine --> GetXCom(model)   --/
                                      |
                                      `-- each request: coordinator IPC
                                          -> supervisor -> Execution API

Each GetXCom uses the current Dag/run and key "return_value".
The first failure cancels the shared pull context and its sibling pulls.
```

The resolved values then follow one of two data-binding plans:

```text
FLAT POSITIONAL INJECTION                 | SOLE-STRUCT INJECTION
------------------------------------------+------------------------------------------
func task(..., country string, cfg Config)| func task(..., input Params)
                 ^ data[0]  ^ data[1]     |                     ^ one data slot
                                          |
ordered bindings / raw values             | named bindings / raw values
  [0] "uk"    -> country                  | region_code="eu-west-1" -> Region
  [1] { ... } -> cfg                      | threshold=0.75          -> Threshold
                                          |
arity must match data slots               | `arg:` exact name or untagged folded name
captured defaults may be dropped          |
                                          | claimed value -> field
                                          | unmatched field -> zero value
                                          | unclaimed explicit arg -> error
                                          | untagged + one explicit arg + no field claim
                                          |   -> decode whole struct

                         BOTH PATHS
                             |
                             v
                   schema compatibility check
                             |
                             v
                   strict JSON decode to Go types
                             |
                             v
                   merge injectable reflect.Values
                             |
                             v
                        function.Call
                             |
                             v
                   result -> return_value XCom
```

- Literal values need no I/O. Only XCom bindings pull the upstream task's `return_value` for the
  current Dag run; independent XCom pulls run concurrently and cancel together on failure.
- Captured Python defaults may remain unclaimed. Flat bindings must match the data-parameter
  count. A sole struct claims arguments by field name, except that one explicit argument may
  decode as the whole value when the struct is untagged and no field matches. Remaining explicit
  arguments and incompatible schemas or Go types fail before the task body runs.
- A known JSON Schema shape is checked against the Go target type before strict JSON decoding.
  Missing or unknown schema forms remain unconstrained and rely on the decoder.
- Cross-language TaskFlow binding uses the coordinator path described in
  [ADR 0003](0003-coordinator-protocol-msgpack-ipc.md).

## Consequences

- The Python Dag remains the single source of task wiring; Go code does not repeat upstream
  `task_id`s or manually pull declared TaskFlow inputs.
- Signature analysis and validation happen during bundle registration. Coordinator mode starts a
  new bundle process per task instance, and each execution still resolves values and invokes the
  function through reflection; the stored plan does not amortize work across task instances.
- Go task returns continue to use the existing `return_value` XCom path, so inbound and outbound
  TaskFlow composition meet at the same language-neutral boundary.
