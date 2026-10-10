<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Dependency readiness preflight

Run before activating a new interpreter, or for the affected profiles of a provider compatibility amendment. Produce a user-visible readiness report before deciding dependency upgrades, provider exclusions, or temporary support limitations. Continue independent prerequisite work while upstream support is pending.

## 1. Plan component probes

For repository-wide addition, inventory core, Task SDK, providers, and other affected distributions/tooling. For provider-only amendments, limit probes to the selected providers, their required dependency closure, affected extras, and reverse dependents. Use component labels `core`, `sdk`, and `provider-<provider-id>`. Plan runtime, relevant extras, and dev/CI profiles separately on promised architectures. Put every promised check in the report plan, including unprobed ones.

An ordinary `uv lock --python <new>` chooses an interpreter, not a target-only resolution range. `uv lock --project <member>` still resolves the entire workspace. Repository-wide additions start with an isolated scratch aggregate attempt, then component probes to attribute failures; provider-only amendments use their scoped component probes directly. Keep source checkout manifests and locks intact.

## 2. Prepare faithful target-specific scratch inputs

Keep scratch manifests, records, and reports under `files/python-readiness/`. Enter Breeze with `breeze shell --mount-sources all` using an existing supported interpreter so both the skill bundle and `files/` are mounted. Run the helper inside that shell.

For a component probe, create a wrapper project depending on the checkout distribution and selected extras, using copied local distribution sources. Copy the reachable first-party dependency closure; convert workspace sources to appropriate paths or a scratch workspace containing only that closure. Temporarily admit the target in copied first-party support metadata and record the original bounds. Leave third-party metadata intact. Keep original requirements, applicable constraints, source overrides, and index policy; runtime probes must not accidentally include unrelated development groups.

Before changing copied support metadata or requirements, save every scratch `pyproject.toml` in `baseline-manifests.json`: a JSON object mapping paths relative to the probe root to their original text. Include the wrapper and every copied distribution manifest. Retain the source commit/diff and original source copies so metadata-build inputs can be reproduced. The helper snapshots this baseline and current manifests for each attempt; altered requirement declarations remain non-ready until a real fix is adopted and a fresh cycle starts.

The helper requires a marked scratch root with an exact target series and matching resolver/wheel-check environments. Adapt this skeleton; replace the example dependency/path with the actual component and its complete selected profile:

```toml
[project]
name = "python-readiness-probe"
version = "0.0.0"
requires-python = "==3.15.*"
dependencies = ["apache-airflow-core"]

[tool.uv]
package = false
environments = ["python_version == '3.15' and sys_platform == 'linux' and platform_machine == 'x86_64'"]
required-environments = ["python_version == '3.15' and sys_platform == 'linux' and platform_machine == 'x86_64'"]

[tool.uv.sources]
apache-airflow-core = { path = "inputs/airflow-core" }

[tool.uv.workspace]
members = []

[tool.python-version-readiness]
component = "core"
profile = "runtime"
platform = "linux/amd64"
python = "3.15"
config-file = "resolver.uv.toml"
```

Use `aarch64` and `linux/arm64` for ARM64. Add copied workspace members where the closure needs them. Put ordinary resolver settings/index definitions, including applicable `exclude-newer`, in the explicit `resolver.uv.toml` beside the manifest; it can be empty when defaults genuinely apply. Project-only metadata such as sources, constraints, and diagnostic exclusions stays in `pyproject.toml`. Carry only overrides relevant to this dependency closure; unrelated workspace overrides must not turn scoped probes red. Relevant dependency overrides are reported conservatively as modified graphs requiring review. The helper records configuration and discards ambient `UV_*` options except the cache location, preventing frozen/stale-lock or user-configuration shortcuts.

Lock success is only resolver evidence. Universal resolution can ignore dependency Python upper bounds; wheel checks do not prove sdists build. Inspect published metadata, target dependency coverage, and later installation/build/tests before declaring support.

## 3. Capture, investigate, and iterate

Capture one attempt per immutable record:

```bash
uv run .agents/skills/python-version-support/scripts/readiness_report.py capture \
  --project files/python-readiness/core-amd64 \
  --record files/python-readiness/core-amd64-01.json
```

The helper runs `uv lock --python <target>` against the marked scratch root, with a default 300-second timeout. Records retain command, uv version, baseline/current manifest snapshots, resolver configuration, exclusions/overrides, changed requirements, before/after locks, exit status, and full stdout/stderr. A lock remaining after failure may be the previous lock; it is not evidence of successful resolution.

For each failed dependency resolution:

1. Read uv's explanation and trace the requirement chain back to its owning component/profile. Identify the actual direct or transitive blocker; every package named in a resolver proof is not necessarily incompatible.
2. Separate Python metadata restrictions, missing wheels/ABI support, incompatible pins, and mutually conflicting requirements from network/authentication, interpreter, build-tool, and unknown failures. Operational failures remain non-ready with their actual diagnostic; do not manufacture dependency exclusions for them.
3. Check compatible published releases and existing upstream issues/PRs. Record exact dependency bounds, the chain, interpreter/architecture, issue links, and a minimal reproduction in `files/python-readiness/upstream-followups.md`. Draft a new issue when no suitable one exists; posting requires explicit authorization and the repository publication workflow.
4. To expose another resolver blocker, add only a confirmed package to scratch `[tool.uv] exclude-dependencies = ["package-name"]` and capture another attempt. This uv setting can omit transitive requirements too. Keep every removed requirement and its original failure in the record set; these hypothetical exclusions are not production fixes.
5. Stop when the unmodified graph succeeds, the diagnostic graph resolves, no new confirmed blocker appears, or the iteration budget is reached (default ten attempts per check). Report incomplete discovery rather than claiming every transitive blocker was found: excluding a branch can hide deeper failures.

Every failed attempt, diagnostic exclusion, dependency override, or changed requirement keeps that check ❌ for the current investigation cycle, even if a later weakened graph resolves. After a real fix, restore the complete intended profile and run a fresh cycle with a new baseline; archive old evidence and upstream follow-ups.

## 4. Render and present the readiness report

Create a plan listing component/profile/platform checks and their attempt records. Paths are relative to the plan file:

```json
{
  "python": "3.15",
  "checks": [
    {"component": "core", "profile": "runtime", "platform": "linux/amd64", "attempts": ["core-amd64-01.json"]},
    {"component": "sdk", "profile": "runtime", "platform": "linux/amd64", "attempts": []},
    {"component": "provider-example", "profile": "runtime", "platform": "linux/amd64", "attempts": ["provider-01.json", "provider-02.json"]}
  ]
}
```

```bash
uv run .agents/skills/python-version-support/scripts/readiness_report.py report \
  files/python-readiness/plan.json --output files/python-readiness/readiness.md
```

The deterministic table has `Component | Ready | Dependency details`, with ✅/❌. Details retain uv's dependency explanation and exclusions, labeled by profile/architecture. The helper does not infer incompatibility from arbitrary package tokens or confuse missing/failed probes with success. Any failed planned check makes its component row ❌; a dev/extra failure is labeled as such rather than attributed to runtime.

Present the report with the upstream tracker and recommendations: dependency upgrades, prerequisite fixes, provider exclusions, or explicit unsupported profiles. Required core/SDK dependencies must be fixed before activation; deleting them from a probe cannot establish support. Use the provider workflow for justified provider exclusions and later enablement.

**Gate:** all promised components/profiles/architectures are represented, blockers have preserved diagnostic chains and confirmed ownership, exclusions are scratch-only, and unresolved or unexecuted coverage is explicit. Resolve required core/SDK blockers before the activation layer; carry justified provider limitations into complete exclusion metadata.

## Optional references

- [1] — Universal resolution and Python-version behavior.
- [2] — Workspace-wide locking.
- [3] — Diagnostic exclusions and target environments.
- [4] — Dependency Python upper-bound limitations.

[1]: https://docs.astral.sh/uv/concepts/resolution/
[2]: https://docs.astral.sh/uv/concepts/projects/workspaces/
[3]: https://docs.astral.sh/uv/reference/settings/
[4]: https://docs.astral.sh/uv/reference/internals/resolver/#requires-python
