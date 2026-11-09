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

# 9. Task loop authoring interface

Date: 2026-11-03

## Status

Proposed.

## Context

A task loop runs a task group again and again until a gate says stop. What a loop means, and the
eight authoring rules every SDK follows, are in
[`airflow-core/adr/lang-sdk/0017`](../../airflow-core/adr/lang-sdk/0017-task-loops-across-lang-sdks.md).
This ADR records the Go spelling of them.

## Decision

1. **`refine.Loop(...)` closes a group.** `dag.TaskGroup("refine")` builds and fills it as usual
   ([ADR 8](0008-native-dag-interface.md)).
2. **`maxIterations` is positional and `Until` is an option.** Every loop has a bound; only some add a
   callable. `airflow.Until(fn, opts...)` is the one `LoopOption`, sealed as `TaskOption` is.
3. **The gate is a task handler wired with `airflow.Inputs`**, which is its own data, as any task's
   is. The terminal task is the body's single leaf, which the SDK orders before the gate. Wiring the
   leaf in is what gets the decision checked against what the body produces, since registration
   already checks a parameter against the recorded output type of whatever `Inputs` names.
4. **`actx.Loop()` supplies the loop context**, beside `Logger`, `Client`, `TaskInstance` and
   `DagRun`. It returns a value and an error, because reading the previous iteration is a round trip.
5. **`Index`, `MaxIterations` and `Previous` are struct fields**, as `DagRun`'s are, and
   `UnmarshalPrevious(out)` binds the raw one. That is `GetXCom`'s split between the value it returns
   and the `out` it decodes into.
6. **A body ref is read from outside the loop unchanged.** The consuming handler declares `T` for one
   iteration or `airflow.XComSequence[T]` for every iteration, checked at registration.
7. **`*LoopRef` is the `Node` that carries edges.** An order-only edge from a body ref to a task
   outside is rejected when it is declared.

## Example

Newton's method for the square root of two, stopping on the error or at ten iterations.

```go
type Metrics struct {
    Estimate float64 `json:"estimate"`
    Error    float64 `json:"error"`
}

func improve(actx airflow.Context) (float64, error) {
    loop, err := actx.Loop()
    if err != nil {
        return 0, err
    }
    actx.Logger().Info("refining", "iteration", loop.Index, "of", loop.MaxIterations)

    estimate := 1.0
    if loop.Previous != nil {
        var previous Metrics
        if err := loop.UnmarshalPrevious(&previous); err != nil {
            return 0, err
        }
        estimate = previous.Estimate
    }
    return (estimate + 2.0/estimate) / 2.0, nil
}

func evaluate(actx airflow.Context, estimate float64) (Metrics, error) {
    return Metrics{Estimate: estimate, Error: math.Abs(estimate*estimate - 2)}, nil
}

func accurateEnough(actx airflow.Context, result Metrics) (airflow.LoopDecision, error) {
    return airflow.LoopDecision{
        Stop:   result.Error < 1e-6,
        Reason: fmt.Sprintf("error %.2g", result.Error),
    }, nil
}

func report(actx airflow.Context, history airflow.XComSequence[Metrics]) error {
    n, err := history.Len()
    if err != nil {
        return err
    }
    actx.Logger().Info("settled", "iterations", n)
    return nil
}
```

```go
dag := airflow.Dag("go_refine", airflow.DagSpec{Schedule: "@daily"})
refine := dag.TaskGroup("refine")

improved := refine.Task(improve)
evaluated := refine.Task(evaluate, airflow.Inputs(improved))

refinement := refine.Loop(10, airflow.Until(accurateEnough, airflow.Inputs(evaluated)))

dag.Task(report, airflow.Inputs(evaluated)) // every iteration's metrics, through the gate
refinement.Before(dag.Task(cleanup))        // order only: the whole loop, not one iteration
```

`evaluated` is written twice and means two things. Inside the loop it is one iteration's `Metrics`,
which is what the gate takes. Outside it is an `airflow.XComSequence[Metrics]`, which is what
`report` takes. Decision 6 is what makes registration accept both.

A loop that only counts needs no callable:

```go
poll := dag.TaskGroup("poll")
poll.Task(checkOnce)
poll.Loop(3)
```

## Signature

```go
package airflow

// Loop turns the group into a task loop. maxIterations must be positive.
func (g *TaskGroupRef) Loop(maxIterations int, opts ...LoopOption) *LoopRef

// Until supplies the gate's callable and the gate's own task options, including the Inputs that
// name the loop's terminal task. fn takes airflow.Context first and the wired inputs after, as
// every handler does, and returns either shape:
//
//    func(actx airflow.Context, args ...) (LoopDecision, error)
//    func(actx airflow.Context, args ...) (bool, error)
func Until(fn any, opts ...TaskOption) LoopOption

// LoopOption is sealed: its only method is unexported, so a loop takes SDK-defined options and
// nothing else.
type LoopOption interface{ applyLoop(*loopConfig) error }

// LoopDecision is what an until callable returns. A bool return is a decision with no reason.
type LoopDecision struct {
    Stop   bool
    Reason string
}

// LoopRef is a Node, so Before and After carry edges for the whole loop.
type LoopRef struct{ /* unexported */ }

// Loop reads the previous iteration's result once and caches it. It panics outside a loop body or
// gate, as the other accessors do on a zero Context, and returns an error when the read fails.
func (c Context) Loop() (Loop, error)

// Loop is handed back by value, as DagRun is: the fields are the runtime's answer, not state for
// a handler to write.
type Loop struct {
    Index         int
    MaxIterations int

    // Previous is the previous iteration's terminal result as it arrived: nil at Index 1, and an
    // XComSequence when the terminal task is mapped.
    Previous any
}

// UnmarshalPrevious binds Previous to a DTO, the way XComClient.GetXCom decodes into its out
// parameter. It leaves out untouched when there is no previous iteration.
func (l Loop) UnmarshalPrevious(out any) error

// XComSequence is the lazy sequence of ADR-0016: every iteration of a loop task read from outside
// the loop, or every instance of a mapped task. It is generic because it is reached as a declared
// parameter type, which the SDK reads by reflection as it does any other parameter.
type XComSequence[T any] interface {
    Len() (int, error)
    At(offset int) (T, error)
}
```

## Consequences

- **A fixed-count loop needs no runtime support.** `Loop(3)` emits `has_until: false`, which core
  completes without a worker (ADR-0017 decision 4).
- **The gate is an ordinary task.** `airflow.Until` takes `TaskOption`, so `airflow.TaskSpec{Queue: ...}`
  lands on it, and its task_id defaults to the Go function name under the group's prefix.
  `TaskSpec{TaskID: "accurate_enough"}` is how it gets a snake_case id, as for any task. A loop with
  no `airflow.Until` has no name to take, so its gate is `__loop_gate`, as Python's is.
- **The body's single leaf is checked twice**: at registration, for the faster message, and by the Dag
  processor, which decides (ADR-0017 decision 3). A nested loop and a loop inside a mapped task group
  are rejected the same way.
- **`actx.Loop()` panics outside a loop**, as the other accessors do on a zero `Context`, so a handler
  moved out of a group fails where it was moved.
- **A mapped terminal needs no second accessor.** `Previous` holds an `airflow.XComSequence[T]` when
  `terminal_is_mapped` is true and a plain value otherwise, which is what Python's one
  `loop.previous` property does. The mapped spelling is a type assertion,
  `loop.Previous.(airflow.XComSequence[Metrics])`, since a sequence is not something to decode into.
- **`Previous` is untyped where `XComSequence[T]` is typed.** A declared parameter type is something
  the SDK reads by reflection, so a sequence carries its element type. A struct field carries nothing,
  and a Go method cannot take a type parameter, so `Previous` keeps `GetXCom`'s shape.

## Alternatives

- **`*Loop` rather than `Loop`.** Rejected: the fields are the runtime's answer, and a pointer invites
  writing to them. `DagRun()` hands back a value for the same reason.
- **A `Loop` parameter on the handler**, as the Java SDK injects one. Rejected: a Go handler already
  takes `airflow.Context` first, and everything else Airflow gives a task hangs off it.
- **Fetching `Previous` before every body handler runs**, which would drop the error from `Loop()`.
  Rejected: a handler that never reads it would pay a round trip from iteration 2 on.
