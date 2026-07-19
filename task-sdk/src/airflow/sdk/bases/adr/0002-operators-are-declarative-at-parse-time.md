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

# 2. Operators are declarative at parse time and only do work at execute time

Date: 2026-07-19

## Status

Accepted

## Context

An operator has two distinct lifecycles, and they run in different processes on
different machines. Its `__init__` runs inside the **Dag File Processor** while it
parses the Dag file — building the declarative graph of tasks. Its `execute()`
(or, for sensors, `poke()`) runs later on the **worker**, when the task actually
runs. `BaseOperator.execute()` deliberately raises `NotImplementedError`: the base
class defines the *contract*, and the subclass supplies the work — in `execute()`,
never in the constructor.

This split has two consequences that are easy to get wrong from inside a single
operator file:

First, `__init__` must be **cheap and side-effect-free**. It runs on the parse
path for every Dag file, repeatedly, under the processor's time and resource
limits. A DB call, a network request, heavy computation, or a function call in a
default argument in a constructor multiplies across every parse and can stall the
processor. The real work belongs on the worker, in `execute()`/`poke()`.

Second, the operator is **declarative data that must serialize losslessly**. The
author's intent is captured on the instance and serialized so the scheduler and
worker can act on it *without re-importing the Dag file*.
`BaseOperator.get_serialized_fields()` derives that field set from a throwaway
`BaseOperator(task_id="test")` instance's `vars()`, minus a curated exclusion set
and plus a curated class-level set — and it must stay in parity with the
airflow-core serialization counterpart (`serialization/definitions/baseoperator.py`
and `schema.json`). A new operator field that is not threaded into that set is set
by the author at parse time, round-trips to a default on the way to the worker,
and is **silently lost** — no error, just an operator that ignores the value.

## Decision

Keep operators declarative at parse time and do real work only at execute time,
and keep the parse-time state serializable.

- **`__init__` stays cheap and side-effect-free** — no DB, no network, no heavy
  work, no function calls in default arguments (ruff `B008`). It records
  declarative configuration only.
- **Real work lives in `execute()` / `poke()`**, which run on the worker; the base
  `execute()` raising `NotImplementedError` is the contract the subclass fulfils.
- **Every new operator field is threaded into `get_serialized_fields()`** — added
  to the derived set (or to the exclusion set with a reason) *and* mirrored in the
  airflow-core serialization definition and `schema.json`, so it survives the
  round-trip to the worker.
- **The round-trip is proven in a test** — construct, serialize, deserialize, and
  assert the authored value survived.

## Consequences

- Parsing stays fast and side-effect-free, so the Dag File Processor is not stalled
  by per-operator construction cost.
- An authored operator value reliably reaches the worker instead of silently
  reverting to a default after serialization.
- Contributors adding a field must touch both the SDK operator and its airflow-core
  serialization counterpart, which the `check-...-in-sync` prek hook enforces.

A change **violates** this decision when it:

- does DB, network, or otherwise heavy / side-effecting work in an operator
  `__init__` (or in a default argument) instead of in `execute()`/`poke()`;
- moves work that belongs on the worker into parse-time construction, or the
  reverse — relying on a constructor side effect that the worker never re-runs;
- adds an operator field that carries authored intent without threading it into
  `get_serialized_fields()` and the matching airflow-core serialization definition,
  so it round-trips to a default and never reaches the worker;
- ships such a field with only an in-process unit test that never exercises
  serialize/deserialize.

## Evidence

- #60619 — "add operator-level `render_template_as_native_obj` override": adds a new
  operator field and threads it through *both* the SDK operator and
  `serialization/definitions/baseoperator.py` + `schema.json` — the exact "new field
  must reach the serialization set" discipline this decision requires.
- #55068 — "Re-enable `start_from_trigger` feature with rendering of template
  fields": a parse-time operator field whose value must survive to the worker and be
  rendered there, wired through the operator and templater together.
- #66979 — "Enable ruff `B008` (function-call-in-default-argument) and fix
  violations": enforces no computed work in constructor default arguments, keeping
  `__init__` cheap on the parse path.
- #62174 — "Order of task arguments in task definition causing error when parsing
  DAG": a construction-time defect that surfaced *at parse time* in the Dag File
  Processor, underlining that `__init__` runs on the parse path.
