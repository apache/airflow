<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Review-imbalance simulation — apache/airflow open PR queue

Read-only simulation of the `pr-management-triage` review-imbalance step over the
open PR queue. **Nothing was posted or changed.** Seeded areas: `jobs`, `cli`,
`docs`, `models`, `serialization`, `executors`, `execution_api`.

Ruleset: softened matrix; strict **in-area** standing; `established` = ≥3 in-area
merged + maintainer-review interaction; `maintainer-already-engaged` exemption
requires **substantive review** (triage nudges excluded); accepted-issue booster
requires the diff to genuinely solve the issue; docstring-only diffs scored as docs;
pre-CLOSE gates (accepted-issue / solicited / engaged) downgrade CLOSE→discuss.

## Population

- Open PRs total: **633**; touching seeded areas: **219**.
- **Ready for maintainer review** (seeded): **105** (of 284 ready; 179 non-seeded
  are out-of-scope pass-through).
- **Not ready** (seeded, not ready-labeled): **114** (21 drafts + 93 open).
  Target population per request = *drafts OR non-maintainer-authored* = **80**
  (34 maintainer-authored non-drafts excluded — all trivially pass via committer
  exemption).

## Simulated actions

### Ready for maintainer review — 85 rigorously re-classified (69 new-area + 16 docs)

| Action | Count | % |
|---|---|---|
| pass | 43 | 50.6% |
| draft-back | 26 | 30.6% |
| discuss-first | 15 | 17.6% |
| recommend-close | 1 | 1.2% |

(20 jobs/cli-only ready PRs carry prior-run verdicts, not re-run with the
substantive-only exemption.)

### Not ready — target set (drafts + non-maintainer), 80 PRs

| Action | Count | % |
|---|---|---|
| pass | 43 | 53.8% |
| draft-back | 18 | 22.5% |
| discuss-first | 12 | 15.0% |
| recommend-close | 7 | 8.8% |

## PASS split — is a maintainer already involved?

| | Ready (43) | Not-ready target (43) |
|---|---|---|
| **Maintainer engaged** | 20 (47%) | 33 (77%) |
| · committer-authored (§5.1) | 2 | 10 (drafts) |
| · under substantive review (§5.4) | 18 | 23 |
| **Rule-based (no maintainer)** | 23 (53%) | 10 (23%) |
| · docs (low criticality) | 16 | — |
| · small-diff / docstring / standing | 7 | 10 |

The step's *own* independent "this is fine" passes are a minority — most passes are
the exemptions correctly staying out of the way of a maintainer who already owns or
is reviewing the PR.

## recommend-close set (8 total — all first-timers, critical/risk-path, no issue, no review)

| PR | queue | area | why |
|---|---|---|---|
| #69734 | ready | execution_api route | first-timer, no linked issue, no maintainer |
| #66411 | not-ready | jobs/triggerer_job_runner | 18-line fix on risk path, first-timer (flagged: maybe discuss) |
| #67077 | not-ready | jobs/scheduler_job_runner | first-timer, no issue |
| #69556 | not-ready | jobs/scheduler_job_runner | HA/loop change, first-timer |
| #69557 | not-ready | jobs/scheduler_job_runner | HA advisory-lock change, first-timer |
| #69705 | not-ready | serialization/encoders + new sdk symbol | first-timer |
| #70051 | not-ready | models/dagrun + ti_deps | first-timer, no issue |
| #70052 | not-ready | models/connection + core_api | first-timer, no issue |

## Notable flags

- Not-ready close rate (8.8%) is ~7× ready (1.2%) — ready PRs already cleared triage
  and drew maintainer engagement; not-ready critical-area PRs from first-timers have not.
- Duplicate / competing PRs surfaced: #67371≡#67831; #70066 vs #70072 (same issue
  #70056, maintainer prefers #70072); #68947 vs #68950 (same issue #68944).
- Pre-CLOSE gate downgraded many first-timer CLOSEs to discuss on real
  linked/good-first-issue implementations: #67637, #68588, #68789, #68863, #69091,
  #69770, #69966, #69991, #70019, #69943.
- Borderline standing calls (strict in-area): #66405, #68139, #69281, #69768, #69840 —
  experienced contributors with a large *overall* record but little in the reference area.
