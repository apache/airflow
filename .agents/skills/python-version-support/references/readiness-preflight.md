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

Lock success is only resolver evidence. Universal resolution can ignore dependency Python upper bounds and admit source distributions without target-compatible wheels. Complete the wheel-detection stage below before new-interpreter image builds; later installation/tests still establish actual support.

## 3. Capture, investigate, and iterate

Capture one attempt per immutable record:

```bash
uv run --project scripts python .agents/skills/python-version-support/scripts/readiness_report.py capture \
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

## 4. Detect dependencies without target wheels

Run before the first new-interpreter image build and initial activation. Audit third-party pins and applicable lower bounds for every promised runtime, extra, and dev/CI profile on both architectures. A recent lock pin can hide a source-only lower bound. First-party workspace sources keep their intended editable installation.

1. **Upgrade before excluding.** For each package without a target wheel, inspect the newest PyPI release and its files on both architectures. Prefer the newest compatible release. When it supplies the missing wheels, add a Python-version-specific lower bound in the owning declaration, including an explicit transitive requirement when needed, and preserve older interpreters' floor, extras, upper bounds, and platform markers. Regenerate metadata/locks and verify both target and older-interpreter lowest-dependency installs.
2. **Persist cooldown exemptions.** Root `[tool.uv] exclude-newer = "4 days"` hides fresh releases, including releases carrying the first target wheels. Identify the exact package and release, then add a dated entry to root `[tool.uv.exclude-newer-package]` outside its generated block, with a comment naming the release and the date to remove the entry. Carry it into scratch resolver configuration. Mirror it in `[tool.uv.pip.exclude-newer-package]` when the pip resolver's separate configuration needs it. CLI `--exclude-newer-package` flags do not persist: a later plain `uv lock` can roll the package back.
3. **Inspect universal-lock caps.** A cap declared on any interpreter's dependency path can hold the universal lock to a shared older version. For example, `apache-beam` 2.76/2.77 declare `pyarrow<26` and `numpy<2.5`. When these prevent target upgrades, use marker-scoped `[tool.uv] override-dependencies` with both a target branch and an other-interpreter branch preserving the original caps. Overrides replace requirements in every environment; a target-only branch can silently remove caps elsewhere. Preserve other existing bounds and verify older interpreters' locked versions after regeneration. Under `uv --resolution lowest-direct` an override requirement is direct, so every branch needs a floor with wheels for its interpreters: a branch without one resolves to the oldest release (`numpy>=1.14.3` made every Low dep job build numpy 1.14.3 from source). Give each interpreter branch the highest floor any member declares for it.
4. **Fork constraints by interpreter when needed.** A marker-scoped `constraint-dependencies` entry can fork the lock, for example `some-package<2; python_version == '3.15'`. Re-resolve and inspect each interpreter's selected version. Never use a constraint to prefer an older release only because it ships a pure-Python fallback wheel: `lazy-object-proxy<1.12` on 3.15 selected 1.11.0, whose `none-any` wheel lacks the C extension, and the pure-Python `Proxy` broke Dag serialization.
5. **Classify sdists.** Missing wheels imply a source build, but compilation is required only when compiled sources (`.c`, `.cc`, `.cpp`, `.pyx`, `.rs`, `Cargo.toml`, `CMakeLists.txt`) are used and there is no optional pure-Python fallback. Inspect the build configuration to establish that condition. PyYAML's `setup.py` falls back to pure Python when its C extension fails; record it as a source build with fallback. Pure-Python sdists need no compiler. Record intentional installer-forced builds, such as `lxml`/`xmlsec` ABI alignment, separately.
6. **Detect compatible wheel tags.** For default Python 3.15, accept `py3` (including `py2.py3`), `cp315` with ABI `cp315`, or `abi3` with a `cp3N` Python tag where N ≤ 15. Platform tags must be `any` or architecture-matching `manylinux`/`musllinux`; check the actual image's libc and baseline before installation. Free-threaded `cp315t` wheels are not required for the default build. Check `Requires-Python` and yanked status too. Binary-only uv probes need explicit Python/platform, an empty target, and `--no-cache` so locally built wheels cannot masquerade as published artifacts.
7. **Run the reusable helper.** Inside a source-mounted Breeze shell, use `uv run --project scripts python` for helpers; use neither bare `python3` nor direct Python execution on the host. [audit_wheels.py](../scripts/audit_wheels.py) consumes target-resolved third-party pins from [CI install filtering](ci-install-filtering.md#follow-the-image-install-path) and reports selected and newest PyPI releases separately for each architecture. It preserves architecture-specific versions, records lookup failures, and flags compiled sources for review. Confirm fallback behavior in each audited release before supplying `--pure-python-fallback pyyaml`.

```bash
uv run --project scripts python .agents/skills/python-version-support/scripts/audit_wheels.py \
  --python-version 3.15.0 \
  --amd64 files/python-readiness/amd64-requirements.txt \
  --arm64 files/python-readiness/arm64-requirements.txt \
  --pure-python-fallback pyyaml \
  --output files/python-readiness/wheel-audit.json
```

For a target-specific floor, split the requirement rather than raising older interpreters' minimum:

```toml
"wheel-dependency>=1.0; python_version < '3.15'",
"wheel-dependency>=2.0; python_version >= '3.15'",
```

Merge entries into existing settings. For an override, retain both interpreter branches and the other interpreters' original cap:

```toml
[tool.uv]
override-dependencies = [
  "pyarrow>=16.1.0,<26; python_version < '3.13'",
  "pyarrow>=18.0.0,<26; python_version == '3.13'",
  "pyarrow>=22.0.0,<26; python_version == '3.14'",
  "pyarrow>=26; python_version == '3.15'",
  "numpy>=1.26.0,<2.5; python_version < '3.13'",
  "numpy>=2.1.0,<2.5; python_version == '3.13'",
  "numpy>=2.4.3,<2.5; python_version == '3.14'",
  "numpy>=2.5; python_version == '3.15'",
]
```

### Trace locked packages, including transitive ones

Run first, before the install-filtered audit exists, and again after lock regeneration. `audit_wheels.py` needs the filtered CI install set, and `scan_wheel_floors.py` reads only direct declarations. [trace_target_support.py](../scripts/trace_target_support.py) walks a `uv.lock` for the target on each architecture instead. It drops edges whose markers fail and follows every member's extras and dependency groups, but only the requested extras of third-party packages. It then audits the locked release and the newest PyPI release of every reachable third-party package. Use the root lock while its target fork is representative; otherwise use a scratch probe lock. Pass every member excluded on the target:

```bash
uv run --project scripts python .agents/skills/python-version-support/scripts/trace_target_support.py \
  --lock uv.lock \
  --python-version 3.15.0 \
  --exclude-member apache-airflow-providers-apache-cassandra \
  --pure-python-fallback pyyaml \
  --cooldown-days 4 --as-of <today> \
  --output files/python-readiness/target-trace.json \
  --markdown files/python-readiness/target-trace.md
```

Each record has the shortest chain from a member, `relation` (`direct` or `transitive`), `declared_by` (members that declare the package, and so own any floor change) and `required_by` (every member that reaches it, which an upgrade unblocks). The table lists lookup errors, locked releases without a target wheel, and packages whose newest release declares the target's `Programming Language :: Python :: 3.N` classifier while the locked one does not. Dispositions:

| Disposition | Meaning | Action |
| --- | --- | --- |
| `upgrade to X` | The locked release has no target wheel; X has one | Verify X on the target, then raise the floor in each `declared_by` member, or for a transitive package in the member nearest the chain's end. If `uv lock` will not select X, a cap is holding it back; use the override branches from step 3 |
| `newer release declares target (X)` | The locked release has a wheel; only X lists the target classifier | Test the locked release on the target; raise the floor only if that fails |
| `… (inside cooldown)` | X is newer than `--as-of` minus `--cooldown-days` | Any floor raise also needs the dated exemption from step 2 |
| `not yet available` | No release has a target wheel: the sdist has compiled sources, or `Requires-Python` excludes the target | Classify the build or the bound, then track it upstream or exclude the owning provider |
| `builds without a compiler` | Pure-Python sdist or confirmed fallback | None |
| `review source` | Git or local path source | Inspect it by hand |
| `lookup error` | PyPI lookup failed (non-zero exit) | Rerun |

To ask what one release unblocks, pass `--package <name>` (repeatable). The table then shows the requested packages even when ready, and `unreached` lists names the target graph never reaches. For example, `--package asgiref` lists the members declaring asgiref and the count of members reaching it, and reports that 3.13.0, the first release to declare 3.15, is newer than the locked 3.12.1.

Classifiers are a soft signal and never change the exit status. A new classifier can be cosmetic: the asgiref 3.12.1 to 3.13.0 diff has no interpreter-specific code change, so the 3.12.1 `py3-none-any` wheel stays acceptable unless target tests fail. The tracer does not replace the install-filtered audit: it does not see installer-forced source builds or the CI image's install filter.

### Raise direct floors to the first release with target wheels

Run after the upgrade-before-excluding pass and again after every exclusion change. `uv --resolution lowest-direct` installs each direct requirement, override requirements included, at its declared floor and resolves transitive packages to their newest release. A floor below the first release with target wheels therefore turns into a source build, and only for direct requirements, so scan the declared requirements instead of a lock:

```bash
uv run --project scripts python .agents/skills/python-version-support/scripts/scan_wheel_floors.py \
  --python-version 3.15.0 \
  --pyproject pyproject.toml airflow-core/pyproject.toml task-sdk/pyproject.toml <every other member not excluded on the target> \
  --output files/python-readiness/wheel-floors.json \
  --markdown files/python-readiness/wheel-floors.md
```

Pass the root `pyproject.toml`, because its overrides are direct requirements, and every member that is not excluded on the target. The helper skips workspace members, direct URLs and constraints, merges all declarations of a package into one specifier, finds the oldest release that satisfies it (pre-releases only when a specifier names one), and reports one status per package:

| Status | Meaning | Action |
| --- | --- | --- |
| `ok` | The lowest selected release has compatible wheels for both architectures | None |
| `raise` | A newer release is the first with wheels; `proposal` is the marker-scoped requirement and `first_wheel_uploaded` its date | Apply the proposal after the checks below |
| `pure-python-sdist` | The floor has no wheel and its sdist has no compiled sources | None; it builds without a compiler |
| `source-only` | No release in range ships wheels | Use the compiled-source evidence, measure the build in the target image, and take the exclusion path only when it is expensive or its toolchain is missing |
| `blocked-by-cap` | Wheels exist only above a declared upper bound | Find the cap's owner and use a per-interpreter override branch |
| `unsatisfiable` | No release supports the interpreter | Take the exclusion path |
| `lookup-error` | PyPI lookup failed (non-zero exit) | Rerun |

Apply each `raise` as a target-specific requirement next to the existing floor in every declaration that owns the package, for example `"msgspec>=0.19.0"` plus `"msgspec>=0.22.0; python_version >= \"3.15\""`. Overlapping markers intersect, so older interpreters keep their floor, extras, caps and markers. Members excluded on the target need no line, and packages reached only transitively need none. Regenerate metadata and locks, and add a dated cooldown exemption for a first-wheel release inside the cooldown window.

Review each proposal before applying it:

- Wheel presence is not runtime support. `dill` 0.4.1 ships a pure-Python wheel that fails on 3.15 because code objects lost `co_lnotab`. Install and import every proposed release in the target image and run the owning tests. When the fix is unreleased, leave a comment at the declaration naming the release that will add support instead of a floor.
- The helper ignores the `none-any` wheel of a project that also publishes interpreter-specific wheels, because it is a fallback without the native extension. `lazy-object-proxy` 1.11 is one: it made `type(proxy).__module__` a property and broke Dag serialization. Such packages report `source-only`; build the newest release from its sdist instead of pinning the fallback.
- Review proposals that cross a major version, such as `sqlalchemy` 2.0 to 2.1. A project with an optional C extension and a pure-Python fallback is better classified with `--pure-python-fallback` in the wheel audit than raised.
- Give every override branch a real floor with wheels for its interpreters, the highest any member declares, because `lowest-direct` resolves a floorless or old-floor override branch to the oldest release.

Verify with a scratch `uv lock --resolution lowest-direct`: each target interpreter's selection has wheels, older interpreters keep their selections, and a rerun of the scan reports no `raise`. Record each old and new floor, first-wheel release and upload date in `wheel-availability.md`.

Save `files/python-readiness/wheel-availability.md` with `Package/version | Owning component/profile and dependency chain | Python/platform | Wheel or sdist evidence | Disposition and target floor | Upstream follow-up`. Distinguish compatible wheels, pure-Python sdists, confirmed fallbacks, compilation requirements, source overrides, and unverified checks. Record old/new floors, release files, cooldown removal dates, and lowest-dependency evidence. The helper's `compiled-sources-review` is provisional until build inspection establishes whether a fallback exists.

Exclude providers with unresolved compilation requirements from initial activation using [provider exclusions](provider-compatibility.md#2-exclude-an-incompatible-provider) and CI install filtering; enable them later when compatible releases/wheels and validation are available. Keep optional-extra limitations scoped where the base provider and promised CI profile remain usable. Required core/SDK/tooling dependencies remain activation blockers when no compatible disposition exists. Re-audit the kept install set after dependency and exclusion changes.

**Gate:** every retained dependency has a verified compatible wheel or an explicitly classified usable source build on each promised architecture. No dependency needs unexpected compilation because a target wheel is missing, and every direct requirement's floor is at or above the first release with target wheels or its source build is classified. New floors, overrides, exemptions, exclusions, and later enablement conditions are recorded; actual installation and `uv pip check` establish metadata compatibility.

## 5. Render and present the readiness report

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
uv run --project scripts python .agents/skills/python-version-support/scripts/readiness_report.py report \
  files/python-readiness/plan.json --output files/python-readiness/readiness.md
```

The deterministic table has `Component | Ready | Dependency details`, with ✅/❌. Details retain uv's dependency explanation and exclusions, labeled by profile/architecture. The helper does not infer incompatibility from arbitrary package tokens or confuse missing/failed probes with success. Any failed planned check makes its component row ❌; a dev/extra failure is labeled as such rather than attributed to runtime.

The helper table reports resolver readiness only. Present the wheel/sdist disposition table alongside it; a resolver ✅ with unverified artifacts or unresolved compilation requirements remains unready for initial activation.

Present the report with the upstream tracker and recommendations: dependency upgrades, prerequisite fixes, provider exclusions, or explicit unsupported profiles. Required core/SDK dependencies must be fixed before activation; deleting them from a probe cannot establish support. Use the provider workflow for justified provider exclusions and later enablement.

For each proposed provider exclusion, record the blocking third-party package names and versions, their full dependency chains, owning runtime/dev/extra profile, and interpreter/architecture failure evidence in the upstream tracker. Use those names as the acceptance checks for [CI image install filtering](ci-install-filtering.md): demonstrate that the provider and its incompatible dependencies are absent from the installed image. Include dev-group chains; runtime metadata alone can miss the actual build blocker.

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
