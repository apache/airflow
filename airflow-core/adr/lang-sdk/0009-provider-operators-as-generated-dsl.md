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

# ADR-0009: Provider Operators as Generated Lang-SDK DSL

## Status

Proposed, and out of scope for the 3.4 timeline. Applies to every Lang SDK; the Go SDK is the worked
example. Records the direction and its feasibility — no generator exists and none is proposed here.

## Decision

1. **Provider operators reach every Lang SDK as generated, serialization-only bindings.** Only a task
   wrapping a host-language function executes in that language; a generated operator carries no body.
   Both go through the SDK's ordinary task registration.
2. **A DSL task runs on a Python worker**, so it must not inherit the SDK's queue, and the deployment
   must have that provider installed. A native Dag is therefore legitimately mixed-execution.
3. **Generate from the Python constructors, commit the output, and guard it with a prek hook.**
4. **Generate an operator only when every required constructor parameter is JSON-serializable**
   (primitive, list, dict, or a nested spec of those); omit optional parameters that are not; skip
   entirely any operator requiring a callable or a live object.
5. **The namespace mirrors `providers/`, adapted to each language's naming rules.**
6. **Templated fields pass through untouched.** The SDK writes the Jinja string; rendering stays
   server-side, where it already happens.
7. **Version skew warns at Dag parsing time and never blocks execution.**
8. **Bindings ship as one package per provider from day one**, on that provider's release cadence,
   with an aggregate pin published alongside.

## Context

The design review on #72043 asked whether authoring a Dag in Go means giving up Python provider
operators. It must not, for any language: Airflow's ~100 provider distributions are what a native
Lang-SDK Dag cannot afford to lose, and what no workflow engine outside Airflow's ecosystem can
offer. A native Dag serializes into the same Dag JSON a Python Dag produces, so any operator whose
constructor arguments are JSON-representable can be expressed from another language as a DSL that
emits serialization and nothing else.

## Example

The Go SDK spelling, mixing a native task with two generated operators:

```go
import (
    "github.com/apache/airflow/go-sdk/airflowprovider/amazon"
    "github.com/apache/airflow/go-sdk/airflowprovider/cncf/kubernetes"
)

extracted := dag.Task(extract) // native Go: runs on a Go worker

staged := dag.Task(amazon.S3ToRedshiftOperator{
    SchemaName: "public", TableName: "events", S3Bucket: "raw", S3Key: "events/{{ ds }}",
}).After(extracted) // DSL only: runs on a Python worker

dag.Task(kubernetes.KubernetesPodOperator{
    Namespace: "airflow", Image: "report:latest", Name: "report",
}).After(staged)
```

`airflow.TriggerDagRun` ([ADR-0008](0008-control-flow-constructs.md)) is the hand-written member of
this family, and the reason a Lang SDK needs no native deferral to offer `deferrable` and
`wait_for_completion`.

## Consequences

- **Authors should pin the DSL to the provider version their deployment has installed.** The bindings
  describe one provider version's constructors, the operator that actually runs is whatever the
  Python worker imports, and skew between them only warns. Matching the two is what keeps that
  warning from becoming a surprise at run time.
- **Most skew is harmless, so it must not be fatal.** Python already fails loudly and precisely when
  a class or argument genuinely is not there, and a parse-time hard failure would take a whole Dag
  out over a version difference its tasks may not even touch. The hook also emits a coverage report,
  so which operators each SDK can reach is reviewable rather than folklore.

## Appendix: Implementation Notes

- **Feasibility.** The shape holds across the tree: ~795 operator classes are declared in the 219
  `operators/` modules under `providers/*/src/airflow/providers/`, and only 9 of those modules
  mention a `Callable` parameter at all (3 a `python_callable`). Sensors and transfer modules add more
  of the same shape. Constructor parameters are ordinary annotated Python arguments, so decision 4 is
  decidable by static inspection.
- **Generation follows an existing precedent.** `go-sdk/pkg/execution/genmodels/models.gen.go` is
  generated from the Python-owned `schema.json` and committed, with CI failing on drift. Generation
  is also the only maintainable path, since each operator's serialized `task_type`/`_task_module`
  must name the real Python class.
- **Namespaces per language.** Java keeps Python's full dotted path
  (`org.apache.airflow.provider.cncf.kubernetes`). Go cannot: a selector is one dot after a package
  name, so `kubernetes.operators.KubernetesPodOperator` is not Go, and Python's
  `operators`/`sensors`/`transfers` layer flattens into one package per provider —
  `airflowprovider/cncf/kubernetes` → `kubernetes.KubernetesPodOperator`. The type names already say
  which kind each is, and all 106 provider distributions have unique leaf names, so no import needs
  an alias. Faking the nesting with a package-level struct value (`kubernetes.Operators.PodOperator`)
  loses per-symbol godoc and struct-literal construction for a familiar-looking dot.
- **Per-provider versioning with a shared prefix**, which every target ecosystem can express:
  - **Python** (the reference): one distribution per provider
    (`apache-airflow-providers-cncf-kubernetes`), sharing the `airflow.providers.*` namespace.
  - **Java**: one Maven artifact per provider, with classes under a distinct sub-package, so nothing
    becomes a JPMS split package; a BOM pins a coherent set.
  - **TypeScript**: one npm package per provider under a shared scope
    (`@apache-airflow/provider-cncf-kubernetes`).
  - **Go**: one module per provider, selected in `go.mod` and tagged with its directory prefix. Only
    the major version reaches the import path, and only from v2; since the tooling reads a `/vN`
    suffix as a version rather than a package name, even a major bump rewrites the import line, not
    the call sites.
- **Why split on day one.** A package boundary is an import path, so collapsing a hundred providers
  into one package now means rewriting every import in every Dag when the split eventually happens.
  The aggregate pin (a Maven BOM, an npm meta-package, a Go module requiring them all, the
  counterpart of Airflow's own `constraints-*.txt`) is what keeps an author from choosing a version
  for each of a hundred packages.
