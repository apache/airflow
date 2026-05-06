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

# ADR-0005: Coordinator Packaging, Module Layout, and Registration

## Status

Proposed — open for discussion. The packaging shipped with the initial Java SDK PRs (`apache-airflow-providers-sdk-java` under `providers/sdk/java/`, registered through `ProvidersManager`) is the *starting point*; this ADR enumerates the alternatives raised on PR #65958 so they can be decided before a second language SDK lands.

## Context

[ADR-0001](0001-java-sdk-airflow-integration.md) introduces a coordinator extension point and ships the Java implementation as an Airflow provider. Reviewers on PR #65958 raised three related but separable questions:

1. **PyPI package name.** Should the Java coordinator ship as `apache-airflow-providers-sdk-java` (consistent with every other provider) or as `apache-airflow-coordinator-java` (recognizing that "language coordinator" is a structurally new kind of distribution that does not behave like operators/hooks/sensors)?
2. **Source-tree module layout.** Should it live under `providers/sdk/java/` alongside other providers, under a nested `providers/coordinators/java/`, or at a new top-level `coordinators/` directory peer to `providers/`, `airflow-core/`, and `task-sdk/`?
3. **Discovery / registration mechanism.** Should coordinator classes be discovered through the existing `ProvidersManager` (and its task-runtime equivalent `ProvidersManagerTaskRuntime`), or through a dedicated `CoordinatorManager` (likely living in a `_shared` library because both Airflow Core and Task SDK need to consume it)?

A related concern raised separately on the same PR is **discoverability and user confusion**: providers appear in the Airflow registry / docs, so "`apache-airflow-providers-sdk-java` exists but `apache-airflow-providers-sdk-go` does not" is visible to end users today (the Go SDK currently ships through Edge-Worker, not as a coordinator). The naming choice affects how prominent that asymmetry is, but it is a **transitional** problem: once Go-SDK migrates to the coordinator interface (planned for 3.3), the asymmetry disappears regardless of which name is chosen.

## Decision (provisional)

**Adopted for the initial PRs:** option **A1** for naming, **B1** for layout, **C1** for registration — i.e., `apache-airflow-providers-sdk-java` under `providers/sdk/java/`, registered via `ProvidersManager`. This is the path of least resistance for landing the SDK and unblocks downstream PRs.

**Open to revisit before a second language SDK lands** (Go-SDK migration): the options below.

### A. PyPI package name

| Option | Name | Argument for | Argument against |
|---|---|---|---|
| **A1** *(current)* | `apache-airflow-providers-sdk-java` | Consistent with the existing provider taxonomy; no new release machinery; user-installation muscle memory (`pip install apache-airflow-providers-…`) carries over. | A coordinator does not expose operators / hooks / sensors / triggers; calling it a "provider" stretches the term. |
| **A2** | `apache-airflow-coordinator-java` | Names the component for what it is — a runtime/coordinator plugin, structurally distinct from a normal provider. Marks it as a new distribution type early, before precedent calcifies (`fab` is the cautionary example reviewers cited). | New distribution type means new release docs, new constraints handling, possibly new versioning conventions vs Airflow Core. Unfamiliar `pip install` shape for users. |
| **A3** | `apache-airflow-sdk-java` | Cleanest from a user perspective — "I'm authoring DAGs in language X, I install the language-X SDK." | Conflicts with the in-tree Python `task-sdk` naming; ambiguous whether the package contains the *user-facing* SDK or the *coordinator* glue. |

### B. Source-tree layout

| Option | Path | Argument for | Argument against |
|---|---|---|---|
| **B1** *(current)* | `providers/sdk/java/` | Already in the providers monorepo conventions; no new top-level directory; ProvidersManager already scans `providers/`. | Visually lumps coordinators together with op/hook/sensor providers. |
| **B2** | `providers/coordinators/java/` | Keeps coordinators inside `providers/` (so existing tooling still finds them) but groups them as a sub-category, signaling that they are not normal providers. | Slight tooling change (provider discovery would need to recurse into the coordinator subtree). Still inherits "this is a provider" framing. |
| **B3** | `coordinators/` (new top-level peer to `airflow-core/`, `task-sdk/`, `providers/`) | Strongest separation; matches the A2 naming. Easier to apply different release / docs rules. | New top-level directory, new uv workspace member, new CI matrix entry. Bigger change for a still-debated decision. |

### C. Discovery / registration

| Option | Mechanism | Argument for | Argument against |
|---|---|---|---|
| **C1** *(current)* | Existing `ProvidersManager` (`airflow-core`) and `ProvidersManagerTaskRuntime` (`task-sdk`) discover the `coordinators` key in `provider.yaml`. | Reuses the discovery pipeline already present in both Core and Task SDK. Zero new infrastructure. | Conceptually couples coordinators to providers even if A2/B3 are picked. |
| **C2** | New `CoordinatorManager`, likely in a shared library (`shared/coordinator-manager/`) so both Core and Task SDK can consume it without import cycles. Coordinators self-register through this manager (e.g., entry-points group `airflow.coordinators` rather than `provider.yaml`). | Decouples coordinator discovery from provider discovery; cleaner separation aligned with A2/B3. Allows coordinator-specific lifecycle hooks (init, version-handshake, capability advertisement) without adding them to `ProvidersManager`. | New shared distribution + its symlink wiring; migration of the Java SDK off `provider.yaml` registration; doubled discovery code paths during transition. |

## Recommendation

The three axes are correlated but not strictly tied. A reasonable consistent set is:

- **Conservative, ship-now:** A1 + B1 + C1 (current state). Lowest risk, lowest change footprint, accepts that "provider" is a slight term-of-art stretch.
- **Aligned-rename:** A2 + B2 + C1. Renames the distribution to `apache-airflow-coordinator-java`, keeps the source under `providers/coordinators/<lang>/`, reuses the discovery infrastructure. This is the cheapest option that clearly *signals* the new component type without re-platforming discovery.
- **Full split:** A2 + B3 + C2. Cleanest end state for when there are multiple language coordinators plus possibly future static-source parsers (cf. the YAML / dag-factory discussion in [ADR-0001](0001-java-sdk-airflow-integration.md#coordinator-interface-subprocess-based-by-design)), but the highest churn and the option that would benefit most from being decided on the dev list as part of a follow-up AIP rather than during the Java-SDK PR review.

The current choice (A1+B1+C1) is intended to be reversible: renaming a not-yet-released distribution and moving its source tree are both mechanical changes. The decision to defer is itself a choice — reviewers who want a different end state should call it out before the SDK ships in `3.3`, not after.

## Consequences

- The Java SDK ships under provider naming/layout in 3.3-preview; if the project later picks A2/B2/B3/C2, the rename becomes a single-PR refactor with deprecation shims (or a hard rename if the SDK is still in preview and we can break unreleased import paths).
- The Go-SDK migration to the coordinator interface (planned for 3.3) is the natural forcing function for a final decision: a second language SDK lands, and the cost of disagreement compounds.
- `ProvidersManager` accumulates an extra extension point (`coordinators`) that is not really an Airflow "provider" responsibility. This is acceptable as a transitional state but is the strongest argument for option C2 over time.
- Documentation must be explicit that "Java appears in the provider list, Go does not" is a transitional quirk, not a stable property. Whichever naming option is picked, release notes for 3.3 should call out the answer for both languages together.
