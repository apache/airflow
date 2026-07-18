---
triage_review_imbalance:
  area: docs
  criticality: low
  review_difficulty: low
  structural_risk_paths: []      # none — docs carry no structural risk
  codeowners_ref: ".github/CODEOWNERS"
  experts: []                    # docs is broadly owned; no dedicated expert gate
  # Inherits the central low ceiling (300 lines / 15 files) — most doc PRs are "small".
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Documentation — Agent Instructions

Prose documentation for airflow-core. Changes here are low criticality and low
review cost: mistakes are visible, reversible, and carry no runtime blast
radius. This area is deliberately welcoming to first-time contributors — the
review-imbalance step should essentially never close a docs PR.

## Why changes here are low cost to review

- No runtime behaviour; the worst outcome is an inaccuracy or a broken build,
  both caught by the docs build check and easily fixed.
- Doc PRs are a common and encouraged **first contribution** — the point of the
  imbalance step is to protect scarce review time on critical code, not to gate
  documentation.

## Conventions a contributor should follow

- Write **Dag** (title case) in prose; keep literal code tokens (`DAG`,
  `dag_id`, `airflow dags list`, …) as-is (see the repo `CLAUDE.md`).
- Build docs with `breeze build-docs` before proposing changes.

## Review criteria

Distilled from what past docs PRs — **both merged and rejected** — actually
needed. **If you are preparing a docs change, apply this checklist _before_
opening the PR.** These are light by design; docs PRs should almost never be
drafted back, let alone closed.

- [ ] **Dag** (title case) in prose; literal code tokens (`DAG`, `dag_id`,
      `airflow dags list`, …) left as-is.
- [ ] `breeze build-docs` passes — no broken build, no dead internal links.
- [ ] Accurate against current behaviour (no stale command/flag/config names).

> This list is a starting point — extend it as new review patterns emerge, and
> add an equivalent `## Review criteria` section to the `AGENTS.md` of every
> other area over time.

## Expectation for large changes

None beyond the normal docs conventions — no discuss-first requirement.
