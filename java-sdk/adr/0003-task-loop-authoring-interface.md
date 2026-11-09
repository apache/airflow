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

# ADR-0003: Task Loop — Java Authoring Interface

## Status

Proposed.

## Context

A task loop runs a task group again and again until a gate says stop. What a loop means, and the
eight authoring rules every SDK follows, are in
[`airflow-core/adr/lang-sdk/0017`](../../airflow-core/adr/lang-sdk/0017-task-loops-across-lang-sdks.md).
This ADR records the Java spelling of them, on both surfaces
[ADR-0002](0002-native-dag-interface.md) defines.

## Decision

1. **`@Airflow.Loop` is `@Airflow.TaskGroup` with a bound.** On the interface surface,
   `dag.loop(groupId, maxIterations)` stands where `dag.taskGroup(groupId)` does.
2. **`@Airflow.Until` marks the gate's method** inside that class. Its absence is a fixed-count loop.
3. **The terminal task is the body's single leaf**, which the SDK orders before the gate. The gate's
   data parameters are bound in `depends()` like any other task's, so binding the leaf is what gets
   the decision checked against what the body produces. The interface surface binds nothing and reads
   the terminal off `Loop` (decision 6).
4. **`Loop` is injected as a method argument**, beside `Client` and `Context` (ADR-0002), into a body
   task or the gate. On the interface surface, where `Task.execute(Context, Client)` is fixed, it is
   `context.loop()`. The processor injects it only where it is declared, so nothing else pays for the
   read.
5. **`index`, `maxIterations` and `previous` are public final fields**, as `DagRun.conf` and
   `DagRun.runId` already are. `previous` is `null` at iteration 1.
6. **`Loop<T>` is generic over the terminal task's result**, so `Loop<Metrics>` gives a typed
   `previous` with nothing to cast. The interface surface has no declaration to read, so
   `context.loop()` is a `Loop<Object>` and `getPrevious(Metrics.class)` names the type at the read.
   `Client.getXCom` gains that same overload, so naming the type at the read is one shape across the
   surface rather than something the loop invents.
7. **A body task's wiring method returns `LoopTaskRef<T>`.** The processor generates every wiring
   parameter of type `XComSequence<E>` as an overloaded pair, one taking `Arg<XComSequence<E>>` and
   one taking `LoopTaskRef<E>`, so `javac` checks whichever reading the consumer declares.
8. **`LoopRef` carries `before` and `after`.** An order-only edge from a body ref to a task outside is
   rejected when the Dag is read.

## Example

Newton's method for the square root of two, stopping on the error or at ten iterations.

```java
@Airflow.Dag(id = "java_refine", schedule = "@daily")
public class RefineEstimate {
  private static final System.Logger log = System.getLogger(RefineEstimate.class.getName());

  record Metrics(double estimate, double error) {}

  // A loop body: a task group with a bound. "refine.improve", "refine.evaluate",
  // "refine.accurate_enough".
  @Airflow.Loop(id = "refine", maxIterations = 10)
  static class Refine {

    @Airflow.Task(id = "improve")
    public double improve(Loop<Metrics> loop) {
      log.log(INFO, "iteration {0} of {1}", loop.index, loop.maxIterations);
      double estimate = loop.previous == null ? 1.0 : loop.previous.estimate();
      return (estimate + 2.0 / estimate) / 2.0;
    }

    @Airflow.Task(id = "evaluate")
    public Metrics evaluate(double estimate) {
      return new Metrics(estimate, Math.abs(estimate * estimate - 2));
    }

    // The gate. Its parameter is the loop's terminal task, bound in depends().
    @Airflow.Until(id = "accurate_enough")
    public LoopDecision accurateEnough(Metrics result) {
      return new LoopDecision(result.error() < 1e-6, "error %.2g".formatted(result.error()));
    }
  }

  @Airflow.Task(id = "report")
  public void report(XComSequence<Metrics> history) {
    log.log(INFO, "settled after {0} iterations", history.length());
  }

  @Airflow.Task(id = "cleanup")
  public void cleanup() {}

  @Airflow.Deps
  static class Wiring implements RefineEstimateDeps {
    void depends() {
      var estimate = refine().improve();
      var metrics = refine().evaluate(estimate);
      refine().accurateEnough(metrics); // the gate, and the terminal it reads
      report(metrics);                  // every iteration's metrics, through the gate
      refine().before(cleanup());       // order only: the whole loop, not one iteration
    }
  }
}
```

`metrics` is written twice and means two things. Inside the loop it is one iteration's `Metrics`,
which is what the gate takes. Outside it is an `XComSequence<Metrics>`, which is what `report` takes.
Decision 7 is what makes both compile:

```java
// Generated by the Airflow Java SDK annotation processor. Do not edit.
interface RefineEstimateDeps extends Deps {
  default Refine refine() { return Flow.loop("refine"); }

  interface Refine extends LoopRef {
    default LoopTaskRef<Double> improve() { return Flow.node("refine.improve"); }
    default LoopTaskRef<Metrics> evaluate(Arg<Double> estimate) { return Flow.call("refine.evaluate", estimate); }
    default TaskRef<Void> accurateEnough(Arg<Metrics> result) { return Flow.call("refine.accurate_enough", result); }
  }

  default TaskRef<Void> report(Arg<XComSequence<Metrics>> history) { return Flow.call("report", history); }
  default TaskRef<Void> report(LoopTaskRef<Metrics> history) { return Flow.call("report", history); }
  default TaskRef<Void> cleanup() { return Flow.node("cleanup"); }
}
```

A loop that only counts needs no gate method at all:

```java
@Airflow.Loop(id = "poll", maxIterations = 3)
static class Poll {
  @Airflow.Task(id = "check_once")
  public void checkOnce() {}
}
```

### Interface surface

```java
Dag dag = new Dag("java_refine").config("schedule", "@daily");

LoopRef refine = dag.loop("refine", 10);
TaskRef improve = refine.task("improve", Improve.class);
TaskRef evaluate = refine.task("evaluate", Evaluate.class);
improve.before(evaluate);
refine.until("accurate_enough", AccurateEnough.class); // terminal: the body's single leaf

TaskRef report = dag.task("report", Report.class);
refine.before(report); // the whole loop: this surface wires no data out of the body
refine.before(dag.task("cleanup", Cleanup.class));
```

```java
public static class Improve implements Task {
  @Override
  public void execute(Context context, Client client) {
    Metrics previous = context.loop().getPrevious(Metrics.class);
    double estimate = previous == null ? 1.0 : previous.estimate();
    client.setXCom((estimate + 2.0 / estimate) / 2.0);
  }
}

public static class AccurateEnough implements UntilTask {
  @Override
  public LoopDecision decide(Context context, Client client) {
    Metrics result = context.loop().getResult(Metrics.class);
    return new LoopDecision(result.error() < 1e-6, "error %.2g".formatted(result.error()));
  }
}

public static class Report implements Task {
  @Override
  public void execute(Context context, Client client) {
    XComSequence<Metrics> history = client.getXComSequence(Metrics.class, "refine.evaluate");
    System.getLogger("report").log(INFO, "settled after {0} iterations", history.length());
  }
}
```

`UntilTask` joins `ConditionTask` and `SwitchTask` as a task kind with its own verb, so the gate is
recognized by the interface it implements rather than by a naming rule.

## Signature

```java
// Annotation surface. @Airflow.Until carries @Airflow.Task's own attributes as well.
@interface Airflow.Loop { String id(); int maxIterations(); }
@interface Airflow.Until { String id() default ""; }

// The gate's method under @Airflow.Until. Its injected arguments come first and its wired data
// after, as any task method's do, and it returns either shape; a boolean is a decision with no
// reason.
//
//   LoopDecision accurateEnough(Metrics result)
//   LoopDecision accurateEnough(Loop<Metrics> loop, Metrics result)
//   boolean accurateEnough(Metrics result)

// Injected into a loop body task or its gate, beside Client and Context. On the interface
// surface it is context.loop(). It throws outside a loop.
// T is the terminal task's result type. Declare it (Loop<Metrics>) and previous is typed;
// declare none (Loop<Object>, which context.loop() returns) and name the type at the read.
public final class Loop<T> {
  public final int index;

  public final int maxIterations;

  /** The previous iteration's terminal result, or null at iteration 1. */
  public final T previous;

  /**
   * This iteration's terminal result, for a gate on the interface surface, where there is no
   * ref to bind it from. Null in a body task. The annotation surface takes it as a parameter.
   */
  public final T result;

  /** previous bound to a DTO, as Client.getXCom's caller names the type it wants. */
  public <R> R getPrevious(Class<R> type);

  public <R> R getResult(Class<R> type);
}

// Added to Client. getXCom returns Object today, so naming the type at the read is new; these are
// the overloads getPrevious and getResult are modelled on. Both take the rest of getXCom's
// parameters, which keep their defaults.
public <T> T getXCom(Class<T> type, String taskId);

// A Class cannot carry XComSequence<Metrics>, so the sequence read names the element type instead.
public <E> XComSequence<E> getXComSequence(Class<E> elementType, String taskId);

/** The lazy sequence of ADR-0016, reached as a declared parameter type or as Loop's T. */
public interface XComSequence<E> {
  int length();

  E get(int offset);
}

/** What an until method returns. A boolean return is a decision with no reason. */
public record LoopDecision(boolean stop, String reason) {}

/** Interface surface. */
public interface UntilTask {
  LoopDecision decide(Context context, Client client);
}

public interface LoopRef extends TaskGroupRef, Flow {
  /** Declares the gate. The terminal task is the body's single leaf. */
  TaskRef<Void> until(String taskId, Class<? extends UntilTask> gate);
}

public class Dag {
  public LoopRef loop(String groupId, int maxIterations);
}

/** Wiring view, generated. A loop body task's ref reads as one iteration or as all of them. */
public interface LoopTaskRef<T> extends TaskRef<T> {}
```

## Consequences

- **A fixed-count loop needs no runtime support.** A `@Airflow.Loop` with no `@Airflow.Until` emits
  `has_until: false`, which core completes without a worker (ADR-0017 decision 4).
- **Neither surface names the terminal.** Both take the body's single leaf and serialize the same
  `terminal_task_id`. The annotation surface binds it into the gate as well, which is what buys the
  compile-time check; the interface surface has no wiring to bind it with.
- **A fixed-count loop's gate is `__loop_gate`.** `@Airflow.Until` carries the gate's id, and a loop
  with no gate method has none to carry, so the id falls back under the group's prefix as Python's
  does.
- **The body's single leaf is checked twice**: by the SDK when the Dag is read, for the faster
  message, and by the Dag processor, which decides (ADR-0017 decision 3). A nested loop and a loop
  inside a mapped task group are rejected the same way.
- **A wrong `Loop<T>` is caught when the Dag is read.** The processor knows the terminal task's return
  type, so `Loop<String>` over a `Metrics` terminal is named as an error rather than failing as a cast
  at run time. `Loop<Object>` is always accepted.
- **The overload pair of decision 7 is generated for every `XComSequence<E>` parameter**, used or not.
  It costs one default method and saves the author a lift at the call site.
- **`context.loop()` throws outside a loop**, so a task class moved out of a group fails where it was
  moved.
- **A mapped terminal needs no second accessor.** `loop.previous` holds ADR-0016's lazy sequence when
  `terminal_is_mapped` is true and a plain value otherwise, which is what Python's one
  `loop.previous` property does. The mapped spelling is the declaration, `Loop<XComSequence<Metrics>>`.
  A `Class` cannot carry that type, so the interface surface reads the sequence with
  `client.getXComSequence(Metrics.class, taskId)`.
- **The interface surface reads the terminal off `Loop`, where ADR-0017 rule 3 reads it as wired
  data.** The deviation is the surface, not the loop: `Task.execute(Context, Client)` is fixed, so
  there is no wiring to bind the terminal with, and `loop.getResult(Metrics.class)` is that surface's
  spelling of the same read. Core pins it to the gate's iteration either way.
- **The interface surface wires no data out of the body**, so ADR-0017 decision 3 has nothing to
  rewire there: a consumer outside the loop orders itself after the loop and reads the sequence
  through `Client`.
- **The reading has to match where the consumer sits.** Core serves only the all-iterations read from
  outside the loop, so a consumer outside it that declares one iteration's value is rejected when the
  Dag is read rather than failing partway through the run.

## Alternatives

- **A getter on an SDK base class**, `getLoop()`, rather than an injected argument. Rejected for the
  reason [ADR-0002](0002-native-dag-interface.md) rejects it for `Client` and `Context`: a task method
  declares what it needs, and the SDK binds it.
- **Fetching `previous` for every body method**, which would let `Loop` be injected everywhere.
  Rejected: a method that never declares `Loop` would pay a round trip from iteration 2 on.
