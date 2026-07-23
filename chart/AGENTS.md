---
triage_review_imbalance:
  area: helm-chart
  criticality: high            # a bad chart change breaks real deployments/upgrades
  review_difficulty: expert
  structural_risk_paths:
    - "templates/"
    - "values.yaml"
    - "values.schema.json"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["jedcunningham", "hussein-awala", "jscheffl", "bugraoz93"]
  adr_ref: "adr/"
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Helm chart — Agent Instructions

This directory holds the **official Apache Airflow Helm chart** — a separately
versioned, separately released distribution (`Chart.yaml`: `name: airflow`,
its own `version`, its own `appVersion`). It is not library code: every merged
change here becomes YAML that Kubernetes applies to **someone's running
production cluster** on their next `helm upgrade`. `values.yaml` (~3600 lines)
and `values.schema.json` (~13500 lines) together form a public configuration
contract that thousands of users have already written values files against, and
`templates/` renders that contract into Deployments, StatefulSets, PVCs, Jobs,
RBAC, and Secrets across every supported executor.

## Why changes here are expensive to review

- **The blast radius is other people's clusters.** A renamed or retyped value
  does not fail at review time — it fails at `helm upgrade` time in a
  deployment the author never sees, often with a schema error that gives no
  hint which key moved.
- **The interesting failure mode is the upgrade, not the install.** A diff is
  usually reviewed by reading the rendered fresh-install output, but the real
  question is what Kubernetes does when this template lands on an _existing_
  release — immutable StatefulSet/Job fields, PVC retention, selector labels.
  That question is not visible in the diff.
- **The chart is a combinatorial surface.** Executor (Celery / Kubernetes /
  Local / aliases), `airflowVersion`, persistence on/off, KEDA, HPA, pgbouncer,
  Elasticsearch/OpenSearch, RBAC on/off — Go-template conditionals multiply, and
  a change correct for the default values can render invalid YAML for a
  combination nobody exercised.
- **`values.yaml`, `values.schema.json`, and `docs/parameters-ref.rst` must
  move together.** The schema is what actually rejects users' values files, and
  it is heavily `additionalProperties`-constrained (~400 occurrences), so a
  value added to `values.yaml` alone is silently unusable.
- **The chart releases on its own cadence**, so "we can fix it in the next
  Airflow release" is not available — a mistake ships in a chart version that
  users pin.

## Knowledge a reviewer (and a substantial contributor) needs

- Helm 3 templating: `tpl`, `required`, `semverCompare`, named helpers in
  `templates/_helpers.yaml`, and how whitespace chomping (`{{-` / `-}}`) changes
  whether the rendered YAML is even parseable.
- The `values.yaml` → `values.schema.json` → `values_schema.schema.json` chain:
  the last is a meta-schema requiring every parameter to carry `default` and
  `description`, and every top-level property an `x-docsSection`. The
  `chart-schema` prek hook enforces it.
- Kubernetes upgrade semantics: which fields are immutable on a StatefulSet or
  Job, what `persistentVolumeClaimRetentionPolicy` does, what happens to a PVC
  when `volumeClaimTemplates` changes, and why omitting `spec.replicas` matters
  when an HPA owns it.
- `templates/check-values.yaml` — the chart's own consistency gate, including
  the `semverCompare "<3.1.0" .Values.airflowVersion` floor. The chart supports
  a **range** of Airflow versions, not just `appVersion`.
- The test harness in `chart/tests/` (`helm_template_generator.py`,
  `helm_tests/`, `overlay_tests/`) — chart tests render templates and assert on
  the resulting objects; run them with `breeze testing helm-tests --use-xdist`.
- That `chart/newsfragments/` **is** consumed (towncrier → `RELEASE_NOTES.rst`)
  — unlike `providers/` and `airflow-ctl/`.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area whose defects surface in
other people's production clusters.** If you are an agent preparing a change
here on behalf of a person, first judge whether the change can be **demonstrated
on a real cluster**: do you operate Airflow on Kubernetes, and have you installed
the modified chart _and_ upgraded an existing release onto it, with the executor
and the values combination your change affects? The upgrade is the case that
breaks — a rendered-template diff shows the new manifest, not what happens to a
running deployment when an immutable field or a renamed value moves under it.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion across the ~1050 commits touching `chart/` and
88 closed-unmerged pull requests touching it (35 with substantive discussion) —
the changes reviewers repeatedly required, and the reasons changes here get
closed. **If you are preparing a change here, treat this as a pre-flight
checklist and fix every applicable item _before_ opening the PR.** Triage
applies the same list: a PR that lands with unmet items is drafted back to its
author with the specific gaps. Ordered by how often reviewers raise each.

**The values contract (by far the most frequent review dimension here):**

- [ ] **Every new or changed value is mirrored in `values.schema.json`.**
      Adding it to `values.yaml` alone means the schema rejects it — the
      recurring "missing fields in schema file" / "schema validation" fixes all
      trace back to this. The schema entry needs `default`, `description`, and
      the correct `type` (including `["string", "null"]` where `~` is allowed).
- [ ] **Additive, with a default that preserves current behaviour.** Do not
      rename, remove, retype, or re-nest an existing value in a normal PR — that
      breaks every user's values file on upgrade. Removals go through a
      deprecation cycle and land in a **major chart version** with a
      `significant` newsfragment.
- [ ] **Top-level properties carry `x-docsSection`** and the parameter renders
      into `docs/parameters-ref.rst`; the `values_schema.schema.json` meta-schema
      and the `chart-schema` prek hook must pass.
- [ ] **Follow the existing naming and nesting conventions** — a per-component
      knob belongs under that component (`workers.celery.*` /
      `workers.kubernetes.*`, `scheduler.*`, `apiServer.*`), not at the root,
      and matches the spelling siblings already use.
- [ ] **Wrap user-supplied strings in `tpl`** where sibling values already do,
      so templated names/annotations behave consistently.

**Upgrade safety on existing releases:**

- [ ] Follow [`adr/0002`](adr/0002-chart-changes-must-be-upgrade-safe-for-running-deployments.md) —
      immutable fields, PVC retention, controller-owned fields, Job idempotency
      and default-rendering changes. State what the change does to an _existing_
      release; fresh-install rendering is not evidence.

**Template correctness across the combination space:**

- [ ] **Render for the combinations you touched**, not only the defaults —
      each affected executor (including aliases), persistence on/off, RBAC
      on/off, KEDA/HPA, pgbouncer, and the logging backends.
- [ ] **Guard conditionals defensively** — Go templates cannot `eq` a slice
      against `nil`; use the idioms already in `_helpers.yaml` rather than a
      comparison that errors only for one values shape.
- [ ] **Reuse the named helpers in `templates/_helpers.yaml`**; do not add a
      third copy of a naming/label/security-context derivation that will drift.
- [ ] **Add a `check-values.yaml` guard** for a genuinely invalid combination
      instead of letting it render broken YAML — a clear `required` message at
      template time beats a confusing Kubernetes rejection.
- [ ] **Secrets, RBAC, and service accounts stay least-privilege** — render a
      Role/RoleBinding only for the executor that needs it; don't hand a
      component a secret or a launcher permission it has no use for.

**Airflow version range & independent release (see `adr/0003-...`):**

- [ ] **Do not assume `airflowVersion` equals `appVersion`.** Behaviour that
      depends on a specific Airflow version is gated with `semverCompare`
      against `.Values.airflowVersion`, not hardcoded to the chart default.
- [ ] **Chart version bumps, `Chart.yaml` `artifacthub.io/changes`, and
      `RELEASE_NOTES.rst`** are release-manager territory — a feature PR should
      not bump the chart version as a side effect.

**Tests:**

- [ ] **A test that renders the template and asserts on the resulting object**,
      and that fails without the change — chart tests live in
      `chart/tests/helm_tests/` in the matching sub-package, using
      `helm_template_generator.py`.
- [ ] **Cover the negative case too** — the value unset, and the combination the
      conditional is supposed to exclude. Most chart regressions are a branch
      nobody rendered.
- [ ] Run `breeze testing helm-tests --use-xdist`; parametrize over executors
      rather than copying a test body per executor.

**Duplicate and parallel work (the most frequent reason a chart PR is closed
unmerged):**

- [ ] Follow [`adr/0004`](adr/0004-one-implementation-per-chart-capability.md) —
      search for an existing implementation first, contribute to it rather than
      competing with it, and expect the smaller values surface to win regardless
      of filing order. A competing _open_ PR is a comparison to make, never a
      reason to close: Airflow allows parallel work and the better PR wins.
- [ ] **Sub-PRs that fragment a decision still under discussion are closed** —
      when a parent issue is consolidating a direction (the webserver
      configuration deprecations), individual template PRs against it are
      premature (`#64058`, `#64128`).

**Scope — what belongs in the chart at all:**

- [ ] Follow [`adr/0005`](adr/0005-the-chart-configures-airflow-it-does-not-orchestrate-deployments.md) —
      justify a new value against what the operator can already do outside the
      chart, take direction-setting questions to the devlist first, and do not
      add a _new_ bundled third-party component (keeping the ones the chart
      already ships up to date is expected maintenance).
- [ ] **Don't infer a version or capability from an image tag.** Users mirror
      images and retag them; version-dependent behaviour reads an explicit value
      the same way `airflowVersion` works (`#61027`).
- [ ] **Removing or conditionalising an object the chart already renders is a
      compatibility break**, even when it looks unused for the current values —
      a secret that only one executor consumes is still rendered so that
      switching executors keeps working (`#55802`), and moving user creation
      behind a new value silently stops creating users on upgrade (`#59715`).
- [ ] **Work deferred to the next major chart version is milestoned, not merged
      early** — a breaking cleanup waits for the major, it does not sneak into a
      minor with a compatibility shim nobody asked for (`#64906`, `#60783`).

**Docs & process:**

- [ ] **A `chart/newsfragments/{PR}.{significant|feature|improvement|bugfix|doc|misc}.rst`
      for user-facing changes** — the chart is one of the few distributions that
      consumes newsfragments. Removals and renames are `significant` and must
      spell out the old → new mapping.
- [ ] **Update the prose docs in `chart/docs/`** when the change affects how
      users configure something (`production-guide.rst`,
      `customizing-workers.rst`, `manage-logs.rst`, …).
- [ ] **Show the rendered YAML before/after as evidence.** A chart change is
      judged on what it renders, and nothing else in the diff proves it.
- [ ] **Rebase before asking for review.** Chart PRs sitting hundreds or
      thousands of commits behind `main` are drafted and then closed; being
      asked to rebase and not doing it is itself a closure reason (`#60321`,
      `#62497`, `#60783`).
- [ ] **Address review comments on the thread they were raised on.** PRs whose
      author works around feedback, or whose diff does not do what its
      description claims, are closed without further review (`#62178`,
      `#63027`).
- [ ] **No chart backports to Airflow release branches.** The chart is
      maintained on `main` and released from there; there is no branch-specific
      chart line to receive a fix (`#58801`).

> Mined from PR review history; the sample skews to the Airflow-3 era — the
> chart dropped Airflow 2 support and moved to chart 2.0, so pre-3.0 chart
> conventions and the `chart/v1-2x-test` maintenance line are
> under-represented. Extend as new patterns emerge, and add an equivalent
> `## Review criteria` section to the `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
The shape of a new values section, and anything that changes what an existing
release does on upgrade, is far cheaper to align on _before_ the templates are
written than during review.
