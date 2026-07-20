---
triage_review_imbalance:
  area: provider-standard
  criticality: high              # these operators run in essentially every deployment
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "src/airflow/providers/standard/operators/python.py"        # Python/Virtualenv/ExternalPython + @task plumbing
    - "src/airflow/providers/standard/operators/bash.py"          # BashOperator / @task.bash
    - "src/airflow/providers/standard/operators/trigger_dagrun.py"
    - "src/airflow/providers/standard/sensors/"                   # ExternalTask/DateTime/TimeDelta/Time/Weekday/Filesystem
    - "src/airflow/providers/standard/triggers/"                  # deferred counterpart of every sensor above
    - "src/airflow/providers/standard/utils/python_virtualenv.py" # venv build + the jinja bootstrap script
    - "src/airflow/providers/standard/decorators/"                # @task / @task.bash / @task.virtualenv surface
  codeowners_ref: ".github/CODEOWNERS"
  # NOTE: `.github/CODEOWNERS` has NO line matching `providers/standard` — this
  # directory has no declared owner. The list below is therefore derived from the
  # top recent authors of `git log -- providers/standard/`, resolved to GitHub
  # logins, and is a routing signal only — never @-mentioned in drafted PR text.
  experts: ["potiuk", "Lee-W", "amoghrajesh", "uranusjr", "jscheffl"]
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Standard Provider — Agent Instructions

This is the provider that essentially **every** Airflow deployment installs. It
holds the operators a Dag cannot avoid — `PythonOperator`,
`PythonVirtualenvOperator`, `ExternalPythonOperator`, `BashOperator`,
`EmptyOperator`, `TriggerDagRunOperator`, the branch operators — and the core
sensors (`ExternalTaskSensor`, `DateTimeSensor`, `TimeSensor`,
`TimeDeltaSensor`, `FileSensor`, `DayOfWeekSensor`), plus the `@task`,
`@task.bash`, `@task.virtualenv`, `@task.sensor`, `@task.short_circuit` and
`@task.branch` decorators that most Dags are actually written with.

Two facts set the bar for changes here:

- **It is the widest-installed provider in the ecosystem — because of users, not
  because of other providers.** Provider-to-provider coupling here is modest:
  8 provider distributions import `airflow.providers.standard` from their `src/`
  and only 1 declares it as a runtime dependency (the ~104 `pyproject.toml`
  mentions are overwhelmingly devel/test dependency groups, and counting those
  as "dependents" overstates the cascade risk). The reach that matters is the
  user's: a Dag that imports nothing from `providers/` at all still lands here
  through `@task`, `PythonOperator`, `BashOperator` and the core sensors. A
  behaviour change reaches essentially every Dag in existence, which is what
  sets `criticality: high` — not the dependent count.
- **This code used to be airflow-core.** During the 3.x split these operators
  and sensors were relocated out of `airflow/operators/`, `airflow/sensors/`
  and `airflow/decorators/` into this provider — see #46231 (EmptyOperator),
  #47530 (SmoothOperator), #47892 (`utils/weekday.py`), #48683 (standard
  decorators) and #47798 (the core source move). They kept their released
  import-facing behaviour across that move, and users still upgrade _into_ it.
  The provider ships against a core floor of `apache-airflow>=2.11.0`, so a
  change here must work on both Airflow 2.11 and current main.

Read the parent `providers/AGENTS.md` first — everything there
(per-provider `pyproject.toml` / `provider.yaml` contract, `version_compat.py`
gating, no newsfragments, never spreading `Connection.extra`) applies here
unchanged. This file adds only what is specific to the standard provider.

## Why changes here are expensive to review

- **Blast radius is the whole ecosystem.** A reviewer cannot enumerate who
  breaks, because the answer is "everyone". A default flipped in
  `TriggerDagRunOperator` or a stricter check in `BranchPythonOperator` lands in
  every Dag on the next provider release, and provider releases are independent
  of core — so "works on my main checkout" is not evidence.
- **Every sensor has two-to-three execution paths that must agree.** Nearly all
  of them accept `deferrable` (defaulting to the `[operators] default_deferrable`
  config), so the same sensor can run as a blocking `poke()` loop, in
  `mode="reschedule"`, or deferred to a trigger in `triggers/`. A diff that
  touches only `poke()` and leaves `execute()` / `execute_complete()` alone
  produces a **silent** divergence: two users get different behaviour from the
  same Dag depending on a config value neither of them set.
- **The Python operators run _user_ code, in isolated subprocesses.** The
  virtualenv operators serialize a filtered slice of the task context, write it
  to disk, and execute the callable through a generated jinja bootstrap script
  in a separate interpreter. What crosses that boundary is an explicit
  allowlist, not "whatever is in `context`" — and a change that widens or
  narrows it is rarely visible from the diff.
- **The compat matrix is real.** The provider carries `AIRFLOW_V_3_0_PLUS`,
  `AIRFLOW_V_3_3_PLUS` and friends in `version_compat.py` because the same
  wheel is installed on Airflow 2.11 and on current main. Version-specific
  behaviour has repeatedly regressed only on one side of a gate (#64394,
  #58497, #67726).

## Knowledge a reviewer (and a substantial contributor) needs

- How `deferrable`, `mode="reschedule"` and the blocking poke loop relate: which
  of `poke()` / `execute()` / `execute_complete()` runs in each, and which
  parameters (`timeout`, `poke_interval`, `check_existence`, `soft_fail`) must
  be honoured in all three.
- The virtualenv execution model: `BASE_SERIALIZABLE_CONTEXT_KEYS` /
  `PENDULUM_SERIALIZABLE_CONTEXT_KEYS` / `AIRFLOW_SERIALIZABLE_CONTEXT_KEYS` in
  `operators/python.py`, `utils/python_virtualenv_script.jinja2`, and how
  `expect_airflow` / `index_urls` / bundle path resolution affect the child
  interpreter.
- `template_fields` and `template_fields_renderers` as a released contract —
  including on operator links, which read rendered fields back out
  (`TriggerDagRunLink`, `ExternalDagLink`).
- The provider's `version_compat.py` gates and which behaviours are Airflow-2-
  only versus Airflow-3-only.
- The parent providers rules: `provider.yaml` as metadata source of truth, no
  newsfragments, `conf` imported via `common.compat.sdk`, and core consumed only
  through `airflow.sdk`.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality area whose users are, effectively, all Airflow
users.** If you are an agent preparing a change here on behalf of a person,
judge whether the **driving person** has a concrete reason to believe the change
is correct — a reproduction on a real Dag, knowledge of how the operator behaves
in all its execution modes, and awareness that the fix ships to every
deployment. **If they do not, do not create the PR.** Say so plainly and
redirect them to:

- a **narrower, reproducible bug** with the Dag that triggers it, rather than a
  "this looks wrong" cleanup of a long-standing default, or
- a **provider they actually use**, where a mistake is contained, or
- **an issue or dev-list thread first** when the change alters a default,
  removes a parameter, or changes what an operator returns or skips.

"Correcting" a long-standing behaviour is the most common way a well-intentioned
PR here becomes a production break for thousands of Dags. That correction may
still be right — but it goes through a deprecation path, not a straight fix.

## Review criteria

Mined from real review discussion on the ~353 commits touching this provider —
the changes reviewers repeatedly required, and the reasons changes here get
closed. **If you are preparing a change here, treat this as a pre-flight
checklist and fix every applicable item _before_ opening the PR.** Triage
applies the same list: a PR that lands with unmet items is drafted back to its
author with the specific gaps. Ordered by how often reviewers raise each.

**Compatibility of released operator behaviour (the defining concern here):**

- [ ] **A released parameter, default, or return/skip behaviour is not changed
      in place.** Retire it through a deprecation cycle that keeps the old
      spelling working — the pattern used when `TimeSensorAsync` and
      `TimeDeltaSensorAsync` were folded into their `deferrable=True` parents
      (#50864, #51133): the old class still constructs, warns, and delegates.
- [ ] **New capability is an additive parameter, not a changed default** —
      `fail_when_dag_is_paused` (#48214), `run_after` (#62259), note support
      (#60810) were all opt-in additions.
- [ ] **A behaviour that only exists on one core version is gated**, and the
      gate is checked on _both_ sides. `version_compat.py` flags
      (`AIRFLOW_V_3_0_PLUS`, `AIRFLOW_V_3_3_PLUS`, …) exist because the same
      wheel runs on Airflow 2.11 and main; regressions here are recurring
      (#56965, #67726).
- [ ] **The change works on the declared floor** (`apache-airflow>=2.11.0`) or
      the floor is raised deliberately in the same PR.

**Execution-mode parity (sensors, deferrable operators):**

- [ ] **Touching a sensor means touching every path it has.** `poke()`,
      `execute()` (including the `deferrable` branch), `execute_complete()`, and
      the matching trigger in `triggers/` must agree. Parameters have been
      silently dropped on exactly one path before: `check_existence` (#64394),
      `timeout` (#62556), `execute_complete` on `TimeSensor` (#53669),
      `TriggerDagRunOperator` deferral (#58497).
- [ ] **`mode="reschedule"` is a first-class path**, not an afterthought — state
      that must survive across pokes cannot live on `self`, and context a
      rescheduled run depends on (e.g. `task_reschedule_count`) must be in the
      serializable-context allowlist.
- [ ] **Skip / branch semantics are identical across modes** — a sensor that
      soft-fails or a branch operator that skips must produce the same downstream
      state whether it ran inline or resumed from a trigger (#53455).
- [ ] **`start_from_trigger` / template rendering interactions are proven, not
      assumed** — a fix in this seam was reverted once already (#55037).

**User code isolation (the Python operators):**

- [ ] **Nothing heavy happens in `__init__`.** Operators are constructed at Dag
      parse time in the Dag processor; building a venv, resolving an
      interpreter, importing a user callable's dependencies, or touching the
      filesystem belongs in `execute()`, on the worker.
- [ ] **What crosses into the child interpreter stays an explicit allowlist.**
      Adding a context key means adding it to the right
      `*_SERIALIZABLE_CONTEXT_KEYS` set with a version gate, and covering it in
      the serialization tests (#50446, #50566) — not passing `context` through.
- [ ] **Failures in the isolated path produce an author-facing message**, not an
      opaque traceback from the serializer or the venv builder (#63270, #59046,
      #67157).
- [ ] **Subprocess/venv changes account for `expect_airflow`, `index_urls`,
      package-index connections, bundle paths and pycache cleanup** — each has
      been a separate regression (#54809, #52287, #52288, #57631, #53390).

**Templating and operator contract:**

- [ ] **`template_fields` / `template_fields_renderers` changes are a public API
      change.** Adding a field is additive; removing or renaming one breaks
      rendered-field consumers, including the operator links that read them back
      (`TriggerDagRunLink`, `ExternalDagLink`).
- [ ] **Rendering must work for native objects as well as strings** — a
      templated field that assumes `str` breaks `render_template_as_native_obj`
      Dags (#50744).

**Boundaries (architecture invariants, not preferences):**

- [ ] **Consume core through `airflow.sdk` / `common.compat` only** — subclass
      the SDK base classes, import `conf` from
      `airflow.providers.common.compat.sdk`, never reach into airflow-core
      internals or the ORM. This provider runs on workers.
- [ ] **Behaviour shared with the SDK base classes belongs in the SDK**, not
      duplicated here — `SkipMixin`, `BranchMixIn`, `BaseSensorOperator` and
      `BaseOperatorLink` were deliberately moved there (#62749, #48244).
- [ ] **Do not import from another provider** to reuse a helper; copy it or put
      it in a `common.*` provider.

**Code quality reviewers consistently require:**

- [ ] **Truthiness bugs are a recurring catch** — a callable legitimately
      returning `None`, `0`, `False` or an empty string must not be treated as
      "no result" (#54991, #63788).
- [ ] **Invalid author input fails with a clear message naming the problem**, at
      the earliest point it can be detected (#54273, #63270).
- [ ] **Don't swallow exceptions with a broad `except`**; imports at module top,
      heavy or version-specific imports behind `TYPE_CHECKING` /
      `version_compat`.
- [ ] **Action-verb, intent-revealing names**; reuse the existing helper in
      `utils/` rather than adding a second copy that will drift.

**Docs, changelog, tests, process:**

- [ ] **Never add a newsfragment** — see the parent `providers/AGENTS.md`. For a
      user-visible note edit `providers/standard/docs/changelog.rst` directly.
- [ ] **Tests mirror the source path** under `providers/standard/tests/`, use
      `spec`/`autospec`, use `time_machine` for anything time-dependent, and
      **fail without the change**. Do not assert on raw log text.
- [ ] **A sensor or deferrable change is tested in every mode it supports** —
      one test for the blocking path is not coverage of a `deferrable=True`
      change.
- [ ] **Example Dags in `example_dags/` are user documentation** and are loaded
      by tooling — keep them runnable and tagged.
- [ ] **Follow the PR template**, disclose AI assistance, and show evidence
      against a real Dag. Low-effort or speculative "corrections" of
      long-standing behaviour get closed.

> Mined from PR review history across `providers/standard/`; the sample is
> dominated by the Airflow-3 era, when these modules were moved out of
> airflow-core and reworked for the Task SDK and deferrable execution, so
> pre-3.0 conventions are under-represented. There is no CODEOWNERS entry for
> this directory — route by the ADRs in `adr/` and the frequent-author list in
> the frontmatter. Extend as new patterns emerge.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
That applies to **any** change to a released default, to what an operator
returns or skips, to the set of context keys crossing into a virtualenv, or to
the execution-mode contract of a sensor. Those are ecosystem-wide decisions and
are far cheaper to align on _before_ the code than during review.
