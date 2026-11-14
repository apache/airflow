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

# ADR-0001: Task Mapping — YAML Authoring Interface

## Status

Proposed.

## Context

What a mapped task is, and the rules every authoring surface follows, are in [Language SDK ADR-0016](../lang-sdk/0016-mapped-tasks-across-lang-sdks.md). This ADR records their spelling in YAML. `map` is deferred since it requires a way to reference a callable (see Decision 4 below).

YAML has no method calls to chain. ADR-0016 makes `zip` and `concat` methods on a reference, so they chain in declaration order. YAML nests them instead. Since YAML already marks a value in place (`{$x: ...}` for an upstream output), an expanded argument is marked the same way:

```yaml
tasks:
  # Returns e.g. [1, 2, 3]
  - id: list_files
    run: {}

  # Equivalent to process.partial(bucket="raw").expand(path=list_files)
  - id: process
    run:
      bucket: raw
      path: {$expand: {$x: list_files}}

  # Reducing; equivalent to summarize(sizes=process)
  - id: summarize
    run:
      sizes: {$x: process}
```

## Decision

1. **An expanded argument is marked in place with `$expand`. Every unmarked argument is a `partial()`.**
   - For example, `path: {$expand: <source>}` under `with:` or `run:` is Python's `.expand(path=<source>)`.
   - Several `$expand` arguments give a cross product, as several `expand()` keywords do. It is not allowed to nest `$expand` inside a construct.

2. **Expansion is possible on a literal list, a `$x`, or one of `$zip` and `$concat`.**
   - A list gives one instance per item. Its items are literal values; a marker inside it is an error. Any other literal is an error.
   - `{$x: task}` expands over the task's return value. When `task` is itself mapped, the instances line up one-to-one by map index. `$x` with a `key` is rejected, as in Python.

3. **`$zip` and `$concat` spell Python's `zip()` and `concat()`.**
   - `{$zip: [a, b, ...]}`, optionally with `default: v` beside the `$zip` key. Without `default`, the zip is as long as its shortest source; with it, as long as its longest, and `v` stands in for the missing items of the shorter ones.
   - A marker's operand is the value of its `$` key, and its options are plain keys beside it. A key that is not one of that marker's options is an error.
   - `{$concat: [a, b, ...]}`.
   - What each takes as an item, and where either may appear, follows Language SDK ADR-0016 (Decision 4 and its authoring rules).

4. **`map()` is deferred until YAML can reference a callable.**
   - YAML cannot define one, and no mechanism yet references a callable in a task handler's language, so `$map` is rejected for now.
   - When one exists, the intended spelling is `{$map: v, apply: f}`: the source is the operand, as for `$zip` and `$concat`, and `apply` is one callable. Nesting `$map` such as `{$map: {$map: v, apply: f1}, apply: f2}` is allowed.

5. **`$expand` as a key of `with:` or `run:` is Python's `expand_kwargs()`.**

     ```yaml
     id: foo
     run:
       path: {$: expand}
       bucket: raw
       size: {$: expand}
       $expand: {$x: rows}
     ```

     is roughly equivalent to

     ```python
     @task.stub
     def foo(path, bucket, size): ...


     foo.partial(bucket="raw").expand_kwargs(rows)
     ```

   - The value of a `$expand` indicates the source, as in Decision 2, except that a literal list's items must be mappings or `$x`. Each element is one instance's keyword arguments.
   - On a `uses:` task, the element's keys are the operator's arguments.
   - On a `run:` task, the `run:` keys are the handler's parameters, in order. `{$: expand}` (Decision 6) marks the ones set by `$expand` elements, and there must be at least one. `{$: expand, default: v}` gives the argument a default, used when an element leaves it out; without one, such an element fails the instance.
   - A task may use either this form, or argument-level `$expand`s, never both.
   - Any other key starting with `$` in `with:` or `run:` is an error.

6. **`{$: ...}` as a keyword marker.**
   - The key `$` marks the value as a keyword naming the marker, for markers that need no argument of their own.
   - `{$: expand}` stands for "this argument comes from each element" and is valid only as an argument's whole value in a `run:` task that has the `$expand` key after merging; a template may carry it for the tasks that extend it (Decision 10). It has one option `default` (see Decision 5).

7. **A reduce is implicit.**
   - `{$x: task}` of a mapped task outside `$expand` gives the consumer every instance's result, as ADR-0016 Decision 8's lazy sequence.

8. **On a `uses:` operator, any constructor argument can be expanded**.
    - This applies whether or not the value is templated. An expanded value is never rendered.

9. **The instance label follows the body.**
    - A `uses:` task keeps `map_index_template`. A `run:` task rejects it; its handler labels itself (ADR-0016 Decision 7).

10. **A template can declare a signature, but cannot map.**
    - A template may contain `{$: expand}`, so tasks can share one handler signature: its arguments, their order, and which ones each element sets.
    - `$expand`, as a key or as an argument's value, is allowed only on a concrete task. Whether a task is mapped is therefore always visible on the task itself.
    - A task that inherits `{$: expand}` but has no `$expand` key is an error naming the template; one that overrides every inherited `{$: expand}` with a value is an ordinary unmapped task.

## More examples

The less common forms, each with its Python equivalent.

```yaml
tasks:
  # a cross product: four instances
  - id: process_grid
    run:
      path: {$expand: [a.csv, b.csv]}
      bucket: {$expand: [raw, eu]}

  # one instance per process instance
  - id: audit
    run:
      size: {$expand: {$x: process}}

  # one [key, value] pair per instance
  - id: tag
    run:
      entry: {$expand: [[env, prod], [team, data]]}

  # .expand(p=list_files.output.zip(process.output, fillvalue=None))
  - id: pair
    run:
      p: {$expand: {$zip: [{$x: list_files}, {$x: process}], default: null}}

  # ProcessOperator.partial(bucket="raw").expand_kwargs(rows.output)
  - id: process_rows
    uses: pkg.ProcessOperator
    with:
      bucket: raw
      $expand: {$x: rows}    # each row's keys are operator arguments

  # BashOperator.partial(env=...).expand(bash_command=commands.output)
  - id: run_commands
    uses: airflow.providers.standard.operators.bash.BashOperator
    map_index_template: "{{ task.bash_command }}"
    with:
      env: {MODE: batch}
      bash_command: {$expand: {$x: commands}}
```

A template can declare a signature for several tasks. Each task supplies its own `$expand`, and an overridden argument keeps the position the template gave it.

```yaml
templates:
  process_sig:
    run:
      path: {$: expand}
      bucket: raw
      size: {$: expand, default: 0}

tasks:
  - id: process_a
    extends: [process_sig]
    run:
      $expand: {$x: rows_a}

  - id: process_b
    extends: [process_sig]
    run:
      bucket: eu                 # replaces the partial in place
      $expand: {$x: rows_b}

  - id: process_one              # the same signature, not mapped
    extends: [process_sig]
    run:
      path: only.csv
      size: 10
```

## Consequences

- **A mapped task reads like an unmapped one with marked values**, and templates and `extends` need no new merge rule.
- **A signature can be shared through a template, but a source cannot.** A task repeats its `$expand` line. Allowing `$expand` in templates later only adds syntax.
- **A row key that names no argument fails the instance on both bodies**: the operator's constructor rejects it on `uses:`, the supervisor on `run:`.
- **A per-instance argument's default is the YAML author's, not the handler's.** `{$: expand, default: v}` becomes the default of ADR-0016's `param` entry, as a Python stub's signature default does. A `default: null` relies on ADR-0016 telling a `None` default from no default, which is fixed there separately. Letting the handler's own default apply instead needs a binding that leaves the argument unset, which ADR-0016 can add later.
- **Near-miss markers get targeted errors.** `{$: expand}` and `{$expand: ...}` look alike and are valid in different places, so each error names the form that fits where it was written.
- **Transforming elements needs a task until `$map` exists.** An upstream task reshapes the list, or a `run:` handler transforms the element it receives.
- **Mapped task groups wait for task groups.** YAML has none yet, so mapped groups and ADR-0016 Decision 4's nested-group shape are out of scope until it does.
- **The loop vocabulary stays free.** Mapping uses only `expand` and Python's source names, so `loop`, `previous`, `result`, `all`, `index` and `until` remain available for Task Loops ([Language SDK ADR-0017](../lang-sdk/0017-task-loops-across-lang-sdks.md)).

## Rejected Alternatives

- **A sibling `expand:` map beside `with:`/`run:`**, with the existing map as `partial()`. This is Python's layout, and `$zip` and `$concat` would need no wrapper. But a task whose arguments come from several `extends` layers could end up with one argument in both maps, which a marker on the value cannot express, and the `expand_kwargs` form would still need its arguments declared in `with:`/`run:`.
- **`$each` and `$item`.** In YAML tools, "each" and "item" mean copies generated when the file is processed (Terraform's `for_each`, Ansible's `loop`), which is not task mapping, and "each iteration" is the language of task loops.
- **`fillvalue` for `$zip`'s option**, Python's name. Python takes it from the standard library's `zip_longest`, which YAML has no counterpart of. `default` names the value for what it is, the stand-in for a missing item.
- **`$map` on `uses:` tasks now**, where the callable runs in the operator's own Python process. It would be a rule that differs by body for a feature meant for both, and adding it later only adds syntax.
- **`expand_kwargs:` as a task-level key**, with the source as its value. It would keep `with:` and `run:` purely arguments, but adds a second mapping word. Decision 5 keeps the partial arguments and the kwargs expansion together, as Python keeps `partial()` and `expand_kwargs()` on one call chain.
- **A per-argument kwargs marker**, `path: {$expand_kwargs: {$x: rows}}` on every argument it supplies. It repeats the source once per argument, and identical sources would have to line up by index, against the cross product that several `$expand` arguments give.
- **Passing the whole element as one argument on `run:`**, as `row: {$expand: {$x: rows}}`, with the kwargs form on `uses:` only. It needs no placeholder, but the handler's signature would differ between mapped and unmapped use, where Python's `expand_kwargs()` leaves it unchanged.
- **Listing the per-instance arguments**, as `args: [path, size]` beside the source. A handler binds by position, and a separate list cannot place an argument among the partial ones.
- **An existing literal as the placeholder**, such as the string `$expand` or null. Every literal already means itself as a value. Reusing one either needs an escape for the ordinary value, or gives it a second meaning in one context, where an accidental or inherited one changes behavior silently. A marker is already the format's way to say "not a literal".
- **Taking the per-instance arguments from the handler's declaration.** The Dag would fail to parse whenever the declaration is unavailable.
- **Spreading the element at run time**, with a new ADR-0016 binding that sends the whole element for each SDK's runtime to spread into its handler's parameters. It needs no declaration in YAML, but is a wire change every SDK must implement, where YAML can declare the parameters itself.
- **An explicit reduce marker.** Python's reduce is implicit, and `$expand` already marks the one-to-one reading, so an unmarked mapped `$x` has only one meaning left.
