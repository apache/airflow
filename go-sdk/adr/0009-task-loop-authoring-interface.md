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
   `DagRun`. It returns a value and an error, because reading the previous iteration is a round trip,
   and outside a loop that error is `airflow.NotInLoop`.
5. **`Index`, `MaxIterations` and `Previous` are struct fields**, as `DagRun`'s are, and
   `UnmarshalPrevious(out)` binds the raw one. That is the split `GetXCom` declares, whose `value`
   parameter is reserved for exactly this decoding and ignored today.
6. **A body ref is read from outside the loop unchanged.** The consuming handler declares `T` for one
   iteration or `airflow.XComSequence` for every iteration, and registration checks which it asked
   for against where it sits.
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

func report(actx airflow.Context, history airflow.XComSequence) error {
    n, err := history.Len()
    if err != nil {
        return err
    }
    var last Metrics
    if err := history.UnmarshalAt(n-1, &last); err != nil {
        return err
    }
    actx.Logger().Info("settled", "iterations", n, "error", last.Error)
    return nil
}

func cleanup(actx airflow.Context) error {
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
which is what the gate takes. Outside it is an `airflow.XComSequence`, which is what
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

// Loop reads the previous iteration's result once and caches it. Outside a loop body or its gate it
// reports NotInLoop, and it reports the read's own error when that fails.
func (c Context) Loop() (Loop, error)

// NotInLoop is what Loop reports for a handler that is not in a loop. Spelled like the SDK's other
// sentinels, sdk.XComNotFound among them: errors.Is(err, airflow.NotInLoop).
var NotInLoop = errors.New("not in a loop")

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
// the loop, or every instance of a mapped task.
//
// It is not generic. The SDK builds it by reflection from a handler registered as fn any, and Go
// reflection instantiates no generic type and synthesizes no method set, so an XComSequence[T] is
// one the runtime could never hand to a handler. The element type is named at the read instead.
type XComSequence interface {
    Len() (int, error)

    // At returns one element as it arrived, as GetXCom returns a stored value.
    At(offset int) (any, error)

    // UnmarshalAt decodes one element into a DTO, as UnmarshalPrevious decodes Previous.
    UnmarshalAt(offset int, out any) error
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
- **`actx.Loop()` reports `NotInLoop` outside a loop** rather than panicking. The other accessors
  hand back a stored field and never panic; only `NewContext` panics, and only on a nil argument. A
  handler moved out of a group still fails where it was moved, through the error it already checks.
- **A mapped terminal needs no second accessor.** `Previous` holds an `airflow.XComSequence` when
  `terminal_is_mapped` is true and a plain value otherwise, which is what Python's one
  `loop.previous` property does. The mapped spelling is a type assertion,
  `loop.Previous.(airflow.XComSequence)`, since a sequence is not something to decode into.
- **The reading has to match where the consumer sits.** Core serves only the all-iterations read from
  outside the loop, so a consumer outside it that declares one iteration's value is rejected when the
  Dag is read rather than failing partway through the run.
- **Nothing in Go carries the element type.** `Previous` is a struct field and `XComSequence` is
  built by reflection, and neither can be generic, so both name the type at the read:
  `UnmarshalPrevious` for one value, `UnmarshalAt` for one element. Go is the only one of the three
  SDKs where the signature does not say it.

## Alternatives

- **`*Loop` rather than `Loop`.** Rejected: the fields are the runtime's answer, and a pointer invites
  writing to them. `DagRun()` hands back a value for the same reason.
- **A `Loop` parameter on the handler**, as the Java SDK injects one. Rejected: a Go handler already
  takes `airflow.Context` first, and everything else Airflow gives a task hangs off it.
- **A generic `XComSequence[T]`**, so the element type sits in the handler's signature. Rejected
  because it cannot be built: handlers register as `fn any`, and Go reflection instantiates no
  generic type, so the runtime has nothing to pass for such a parameter and nothing to assert
  `Previous` to.
- **Fetching `Previous` before every body handler runs**, which would drop the error from `Loop()`.
  Rejected: a handler that never reads it would pay a round trip from iteration 2 on.
