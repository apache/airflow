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

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — pr-management-triage review-cost imbalance step

Detailed logic for the **review-cost imbalance** step declared in
[`pr-management-triage.md`](pr-management-triage.md). This step
answers one question per PR:

> Is this change **cheap to produce but expensive to review
> properly**, landing in a **critical / hard-to-review area**, from
> an author who has **not demonstrated standing in that area** —
> such that accepting it into the review queue is a poor use of
> scarce maintainer review time?

When the answer is a strong yes, the step proposes closing the PR
with a **mentorship message** (below) that invites the contributor
to first build standing through discussion and smaller changes.

When it is **not** a clean close but the PR fails the area's
**review criteria** (§2b) — the things past reviews in this area
have historically flagged — the step instead proposes to
**draft the PR back to the author**: convert it to draft, assign the
author, and fold the criteria findings into the PR body (deduped
against feedback already given and the author's prior responses).
This readiness gate runs **before** a PR is promoted to
`ready for maintainer review` — a PR that fails area criteria never
enters the reviewable queue in the first place.

The step **never acts autonomously** — it only adds a proposed
disposition the maintainer fires, exactly like every other action
in `classify-and-act.md`.

> **Framing guard.** This is a *review-economics* judgement, not a
> judgement of the author or the code's merit. The mentorship
> message must never assert the code is wrong or the author
> unwelcome — only that a change of this cost, in this area, is
> better preceded by discussion. When in doubt, prefer
> `discuss-first` over `recommend-close`, and prefer `pass` over
> `discuss-first`.

---

## 1. Inputs

Per PR, the step consumes:

| Signal | Source |
|---|---|
| Changed file paths | PR record already fetched in Step 1 (session cache `fetched_prs.all_prs[].raw`) |
| Diff magnitude (lines added+removed, files changed) | Same PR record; `gh pr diff --stat` only if absent |
| Author login + association | PR record (`authorAssociation`) |
| Labels, linked issues, PR/issue timeline | PR record + `gh` (only for candidates that survive exemptions) |
| Area metadata (criticality, difficulty, knowledge) | Nearest-ancestor `AGENTS.md` of each changed path — see §2 |
| Author standing in area | On-demand `gh` queries — see §4. Computed **only** for PRs that reach the scoring stage (post-exemption), to keep query volume low. |
| Area **review criteria** (what past reviews flagged) | The `## Review criteria` section of the area's `AGENTS.md` — see §2b; optionally enriched on-demand from recent merged + rejected PRs in the area |
| Existing folded feedback + author responses | The managed feedback marker block in the PR body + the PR/issue comment timeline — see §6d, so past feedback is never repeated |

---

## 2. Area metadata — `AGENTS.md` frontmatter

Each area root carries an `AGENTS.md` with a namespaced frontmatter
block owned by this step. Human prose (the existing AGENTS.md
convention) stays below it, unchanged.

```yaml
---
triage_review_imbalance:
  area: scheduler-jobs           # stable slug, for reporting
  criticality: high              # low | medium | high | critical — BASE tier for the area
  review_difficulty: expert      # low | medium | high | expert
  # Sub-path tiering. Paths within the area carrying extra structural risk —
  # the hot/central code where a subtle mistake is most costly. A changed file
  # matching any of these globs (matched against the path tail under the area
  # root) is treated as criticality=critical for BOTH ReviewCost and the
  # small-diff ceiling (§3), regardless of the area base above. This is how a
  # peripheral change (metrics, logging) scores lower than a scheduler-loop
  # change without splitting the directory into many AGENTS.md files.
  structural_risk_paths:
    - "scheduler_job_runner.py"
    - "triggerer_job_runner.py"
    - "job.py"
  # Area maintainers / CODEOWNERS. INTERNAL SIGNAL ONLY — used to recognise
  # maintainer engagement (§5) and to weight "review comments accepted by an
  # area expert" in AuthorStanding (§4). NEVER @-mentioned in drafted PR text
  # (the repo's do-not-tag-individuals rule); drafts say "an area maintainer".
  codeowners_ref: ".github/CODEOWNERS"   # source of truth; experts below is a cached view
  experts: ["ashb", "XD-DENG"]
  # Optional explicit small-diff ceiling override; omit to inherit the
  # criticality-driven default (§3).  small_diff: { max_lines: 10, max_files: 1 }
---
```

**Resolution.** For each changed path, walk up to the nearest
ancestor directory that has an `AGENTS.md` with a
`triage_review_imbalance` block. A file with **no** such ancestor
inherits the fallback tier `criticality: medium`,
`review_difficulty: medium` (a conservative default — unknown areas
are treated as neither trivial nor extreme). A plain-prose
`AGENTS.md` with no `triage_review_imbalance` block is treated as
"no metadata here" and the walk continues upward.

**Aggregation across a multi-area PR.** Take the **max**
`criticality` and **max** `review_difficulty` over all touched
areas (ordering: `low < medium < high < critical`,
`low < medium < high < expert`). Record **breadth** = the count of
distinct areas touched.

> The seeded metadata is a *draft for maintainer review*, not
> ground truth. Under-tagging (calling a critical area `medium`)
> only makes the step more lenient; over-tagging makes it stricter.
> Tune from the backtest (§7).

## 2b. Review criteria (per-area, mined from history) — dual-use

Each area's `AGENTS.md` carries a **`## Review criteria`** section: a
concrete checklist of what reviews in this area have historically
flagged, distilled from **past PRs — both merged and rejected**.
Merged PRs show what a good change in the area looks like and what
reviewers ultimately required; rejected/closed PRs show the
recurring reasons changes here fail. Typical entries: "a change to X
must keep backward compatibility with Y", "touching Z requires a
migration + a test at both versions", "new public surface needs a
newsfragment", "don't bypass the Execution API from this path".

This section is the **single source of truth**, consumed from both
sides:

- **Authoring side (before a PR exists).** An agent preparing a
  change in this directory reads `AGENTS.md` — which it does anyway
  — applies the `## Review criteria` as a **pre-flight self-review
  and fixes the gaps *before* opening the PR**. The criteria are
  written as author-facing checks for exactly this reason. This is
  the "apply the criteria before the drafts are in" requirement:
  the cheapest place to satisfy a review criterion is before the PR
  is ever created.
- **Reviewing / triage side (this step).** The same checklist drives
  the §6 draft-back assessment — a PR that skipped the pre-flight
  and lands with unmet criteria is drafted back to the author with
  the specific gaps folded in.

**Resolution & fallback.** Same nearest-ancestor walk as §2. An area
with no `## Review criteria` section falls back to the **generic
cross-area checklist** (tests for changed behaviour, no architecture-
boundary violation, backward compatibility, docs/newsfragment when
user-facing, no new `raise AirflowException`) — enough to catch the
common misses, but the area-specific list is where the real value
is. **Every area's `AGENTS.md` should grow its own `## Review
criteria` section over time** — the seeded ones are a starting
point; extend to other areas as review patterns emerge.

**On-demand enrichment (optional).** When assessing a borderline PR,
the step may sample the most recent merged and closed-unmerged PRs
touching the reference area (`gh pr list --search`) to confirm a
criterion still holds or surface a newer one — but the `AGENTS.md`
section remains the durable record; transient findings should be
proposed back into it, not left only in a PR comment.

---

## 3. ReviewCost

`ReviewCost ∈ {low, moderate, high, extreme}`.

Start from the **effective criticality**, then adjust for size,
breadth, and structural risk.

**Effective criticality per changed file** = the area base
`criticality`, **but promoted to `critical`** if the file matches any
of the area's `structural_risk_paths` globs (matched against the path
tail under the area root). Then take the **max** effective criticality
over all changed files — this, with `review_difficulty`, gives the
base. So a metrics tweak in `jobs/` (base `high`) scores `high`, while
a change to `scheduler_job_runner.py` (a `structural_risk_paths` match)
scores `critical` → `extreme`, even in the same directory.

**Base from effective criticality** (max over touched files/areas):

| criticality | review_difficulty | base |
|---|---|---|
| critical | expert / high | extreme |
| critical | medium / low | high |
| high | expert / high | high |
| high | medium / low | moderate |
| medium | any | moderate |
| low | any | low |

**Adjustments:**

- **Large diff** (≥ 400 changed lines *or* ≥ 15 files): bump one
  tier (capped at `extreme`).
- **Broad** (breadth ≥ 3 distinct non-`low` areas): bump one tier.
- **Structural risk** present — either a changed file matched an area's
  `structural_risk_paths` (already reflected via effective criticality
  above), or a cross-cutting risk category: new/changed public REST API
  surface, Execution-API version change, DB migration, serialization
  format change, new public SDK/`airflow.sdk` symbol, security-path
  change: bump one tier.

**Small-diff exemption (criticality-scaled).** If the PR's diff is
under **both** ceilings for its **effective** criticality (so a
`structural_risk_paths` file uses the `critical` row even when the area
base is lower), `ReviewCost` is pinned to `low` regardless of the
above. Ceilings (central default; a per-area `small_diff` in frontmatter
overrides its own area's row):

| criticality | max_lines | max_files |
|---|---|---|
| low | 300 | 15 |
| medium | 100 | 6 |
| high | 30 | 3 |
| critical | 10 | 1 |

A 40-line scheduler-loop change is therefore **not** small (critical
ceiling is 10/1); a 40-line docs change **is** small (low ceiling
is 300/15). This is the "small diff should depend on criticality"
rule.

---

## 4. AuthorStanding

`AuthorStanding ∈ {none, weak, established, trusted}`, measured
**in the area(s) the PR touches** (use the highest-criticality
touched area as the reference area for the queries).

| Signal | How to read it | Query |
|---|---|---|
| **Committer / PMC / tenure** | `authorAssociation ∈ {MEMBER, OWNER}` ⇒ committer/PMC. `COLLABORATOR` ⇒ established. | PR record (free) |
| **Merged PRs in area** | Count of the author's *merged* PRs that touched files under the reference area root. | `gh pr list --author <login> --state merged --search "<area path terms>"` then confirm file overlap on the top hits |
| **Accepted in-area review comments** | Author left review comments on *others'* PRs in the area that were acted on / resolved (a proxy for "their comments have been accepted"). Extra weight if an area **`expert`** (frontmatter) engaged with or approved alongside them. | `gh search` / `gh api` over review comments authored by `<login>` on PRs touching the area; count threads that were resolved or followed by a matching change |
| **Prior design discussion** | Author opened or substantively participated in an issue / AIP / devlist thread about the area before this PR. An **accepted-issue / AIP link on this PR** counts here as a booster — **but only when the PR actually solves that issue** (see the qualification below), not merely links it. | Linked-issue timeline + `gh search issues --author <login>` in the area |

**Accepted-issue booster — qualification (important).** A `Closes
#N` / `Fixes #N` link by itself does **not** earn the booster or the
pre-CLOSE gate (§6). The link must be corroborated by evidence that
the change *genuinely resolves* the referenced issue, either:

- a **reviewer / maintainer comment** on the PR affirming it
  addresses the issue (e.g. "this fixes it", an approving review that
  references the issue, "confirmed the repro is gone"), or
- the **diff demonstrably implements** what the issue describes (the
  changed code clearly matches the issue's described fix/feature — a
  judgement the step makes from the diff, not from the link alone).

A linked issue that is stale, unrelated, mis-scoped, or that the PR
only partially/tangentially touches does **not** qualify. This stops
the booster from being gamed by dropping a `Closes #N` on an
unrelated large drive-by. When the evidence is genuinely unclear,
treat the booster as **not** earned for AuthorStanding, but still let
the §6 pre-CLOSE gate hold the PR open for a human to judge rather
than closing it (bias against a wrong close).

**Tiering (take the strongest evidence):**

- `trusted` — committer / PMC (auto), *or* an `established` author
  whose engagement is specifically **in-area** (several in-area merged
  PRs **and** accepted in-area review comments).
- `established` — a **genuine engagement track record**: **several
  (≥ 3) merged PRs** **and** a **history of discussion with
  maintainers** — PRs where a committer/PMC reviewed and the author
  engaged with and addressed the feedback (not just merged trivial
  changes untouched). In-area merged PRs and accepted in-area review
  comments count extra toward this; `COLLABORATOR` qualifies directly.
  **A single incidental merged PR does NOT qualify** — that is `weak`.
- `weak` — some history but short of that bar: only one or two merged
  PRs, merges with no real maintainer-review interaction, or history
  only in unrelated areas.
- `none` — first-time or near-first-time contributor with no
  footprint of any kind.

> Rationale for the raised `established` bar: the point of standing is
> to distinguish someone who has *demonstrated how they think and
> communicate with maintainers* from someone who happens to have one
> merge. A lone incidental PR is not that evidence; sustained
> merges + real review back-and-forth is.

> Missing/ambiguous signal resolves **upward** (toward more
> standing), never downward — the cost of wrongly closing a
> legitimate contribution is far higher than the cost of letting a
> borderline PR through to normal review.

---

## 5. Hard exemptions (short-circuit before scoring)

If **any** holds, emit `pass` immediately and do not query standing:

1. **Committer / PMC author** — `authorAssociation ∈ {MEMBER, OWNER}`
   (cross-checked against `committers_team` in
   [`pr-management-config.md`](pr-management-config.md)).
2. **Small diff** — under the criticality-scaled ceiling in §3
   (i.e. `ReviewCost` pinned to `low`). (Exempts the **close** path;
   does not exempt the criteria gate — a small PR can still miss a
   required test.)
3. **Maintainer-solicited** — a committer asked for the change: an
   issue assigned to the author by a committer, a committer comment
   inviting the PR ("please send a PR", "go ahead and open one"), or
   the PR implements an AIP the author was asked to implement.
4. **Maintainer already engaged + author responding** — a committer
   has already reviewed / commented on this PR *and* the author has
   responded. The review conversation is underway; the imbalance
   step must not cut across a maintainer's in-flight engagement.

---

## 6. Decision

Two assessments combine into one disposition.

**(a) Imbalance verdict** — from `ReviewCost` (§3) ×
`AuthorStanding` (§4). The extreme row is softened so that a
non-trivial change from an author with *some* standing is routed to
work-it-out paths (draft-back / discuss) rather than closed outright;
only `none`-standing authors are closed:

```
                          AuthorStanding →
ReviewCost ↓     none        weak         established   trusted
extreme          CLOSE       draft-back   discuss       pass
high             CLOSE       discuss      pass          pass
moderate         discuss     pass         pass          pass
low              pass        pass         pass          pass
```

The matrix can therefore emit `draft-back` directly (extreme×weak);
the criteria assessment below can *also* raise `draft-back` on an
otherwise-`discuss`/`pass` verdict.

**(b) Criteria assessment** — evaluate the PR against the area's
`## Review criteria` (§2b). Classify each unmet criterion as
**blocking** (would prevent merge: missing test for changed
behaviour, backward-compat break, missing migration, architecture-
boundary violation, missing newsfragment on a user-facing change)
or **minor** (style/nits). Let `criteria_gaps` = the set of unmet
**blocking** criteria, after §6d dedup against feedback already
given and the author's prior responses.

### Combined procedure (in order)

Applied at the **readiness decision, before any `mark-ready`** —
a PR that fails here never enters the reviewable queue.

1. **Exemptions (§5).** Committer/PMC and "maintainer already
   engaged + author responding" short-circuit to `pass` for **all**
   paths below. Small-diff and maintainer-solicited exempt the
   **close** path only — they do **not** exempt the criteria gate.
2. **CLOSE** (matrix verdict `CLOSE`) → **first apply the mandatory
   pre-CLOSE gates**; if none fire, disposition
   `recommend-close (review-imbalance)`; mentorship message (§6a).
   The only outright-close path.
   **Pre-CLOSE gates — if any holds, DOWNGRADE `CLOSE` → `discuss`
   (never close):**
   - the PR **qualifies for the accepted-issue booster** (§4
     qualification — links an issue *and* genuinely solves it, per
     review or diff evidence);
   - the PR is **maintainer-solicited** (would also be a §5 exemption);
   - a maintainer is **already engaged** and the author is responding.
   These gates exist because the fast objective matrix over-closes:
   the signals that most often make a CLOSE wrong (a real linked+solved
   issue, a solicited change) must be evaluated for real before any
   close is proposed — they are not optional boosters at close time.
3. **Draft-back** — triggered when **either** the matrix verdict is
   `draft-back` (extreme×weak) **or** the verdict is `discuss`/`pass`
   but `criteria_gaps` is non-empty → disposition
   `draft-back (review-criteria)`: convert to draft, assign the author,
   fold into the PR body (§6c). Supersedes `discuss`/`mark-ready`: the
   PR is not ready but not closable either — it goes back to the author.
   The folded note carries the `criteria_gaps` when present; for a
   matrix-driven draft-back with no gaps, it carries the imbalance
   rationale instead (high-cost area; build standing / discuss first).
   The §6d "empty gaps ⇒ don't draft-back" rule applies only to the
   *criteria-driven* trigger — a matrix `draft-back` verdict always
   drafts back.
4. **Discuss** (matrix verdict `discuss`, no blocking gaps) →
   disposition `discuss-first (review-imbalance)`; comment (§6b).
   PR stays open.
5. **Pass** — no blocking gaps, verdict `pass` → the default
   `suggested_action` stands (eligible for `mark-ready`).

The maintainer fires every non-`pass` disposition through the normal
`interaction-loop.md` confirmation flow. Record the verdict, the
`(ReviewCost, AuthorStanding)` pair, the `criteria_gaps`, and the
touched areas in the session cache for the history gist and the
backtest.

### 6a. Mentorship message (CLOSE)

Body posted as the closing comment. Fill the slots; keep the tone
factual and non-judgemental. Refer to **roles**, never `@`-mention
individuals (per the repo's "do not tag individuals" rule).

```markdown
Thank you for the contribution, and for the time you put into this.

I'm closing this PR for now — not because of the code itself, but
because of how this part of Airflow works. Changes to **<area>** are
among the most expensive to review correctly: <one line on why —
e.g. "they interact with scheduler concurrency and the task
state machine, where a subtle mistake can stall or corrupt runs
across every deployment">. For changes of this size and impact in
this area, we ask contributors to first build shared context with
the maintainers of the area.

The best way to do that is to **start smaller and discuss first**:

- Open an issue (or reply on the dev list) describing the problem
  and the approach you have in mind, and let the discussion shape
  the design before the code.
- Land a few smaller, self-contained changes in this area first —
  and reviewing others' PRs here is just as valuable, because it
  shows how you reason about the area and how you communicate about
  it.

This isn't a rejection of the idea — it's an invitation to build the
context that makes a change this large reviewable. Once there's a
shared understanding of the approach, a PR like this is much easier
for a maintainer to take on. Please do come back.

<links: contributing guide / relevant area doc>
```

### 6b. Discuss-first comment (discuss)

PR stays open; nudge toward discussion without closing.

```markdown
Thanks for this! Before we dig into a full review — changes in
**<area>** carry enough review cost that it's worth aligning on the
approach first. Could you open (or link) an issue / dev-list thread
describing the problem and the design you're going for? That lets us
shape the direction before the detailed review, and usually gets the
change merged faster in the end.

<links: relevant area doc>
```

Both messages end with the framework's standard attribution footer
per [`comment-templates.md`](../../.claude/skills/pr-management-triage/comment-templates.md)
— `reviewed by @<maintainer> before posting` when the maintainer
confirmed the draft, the no-review form otherwise.

### 6c. Draft-back mechanics (`draft-back (review-criteria)`)

On maintainer confirmation, three actions, in order:

1. **Convert to draft** — `gh pr ready --undo <n>` (no-op if already
   a draft).
2. **Assign the author** — `gh pr edit <n> --add-assignee <login>`,
   so the ball is visibly in the author's court and the PR leaves the
   maintainer's actionable queue.
3. **Fold the criteria gaps into the PR body** — write them into the
   managed feedback marker block (the `triage_feedback_channel:
   pr-body` mechanism from [`pr-management-config.md`](pr-management-config.md)),
   **not** a new comment. Editing the body does not notify
   subscribers, keeping the mailbox quiet; the draft-conversion +
   assignment are the signal to the author. Only the §6d dedup output
   goes in.

Folded block content:

```markdown
This PR was moved back to **draft** because a few things this area
expects aren't met yet. Once these are addressed, mark it ready for
review again and it'll come back into the queue.

**Review criteria for `<area>` not yet met:**

- [ ] <specific unmet criterion, tied to the file/line where it applies>
- [ ] <…>

These come from what changes to this area have historically needed —
see the `Review criteria` section of `<area>/AGENTS.md`. (An agent
preparing changes here can apply that same checklist up front to
avoid this round-trip.)
```

**Matrix-driven draft-back with no criteria gaps** (extreme×weak, clean
PR): fold the imbalance rationale instead of a checklist —

```markdown
This PR was moved back to **draft**. Changes in **<area>** are among
the most expensive to review, and this is a substantial change from
someone still building context here. That's not a rejection — before a
maintainer commits the review time, it's worth aligning on the approach
(an issue or dev-list thread) and/or landing a smaller change in this
area first. Mark it ready again when you'd like it re-queued.
```

### 6d. Accounting for past responses (dedup before folding)

Before folding anything, read (i) the existing managed feedback
marker block in the PR body and (ii) the PR/issue comment timeline.
Then:

- **Drop** any criterion already raised in a prior folded block or a
  prior review comment — never restate it.
- **Drop** any criterion the author has already **responded to or
  addressed** — if a later commit or a reply from the author speaks
  to it, treat it as handled; do not re-raise. When unsure whether a
  response resolved it, leave it out and let a human reviewer judge
  (bias to *not* nagging).
- If, after dedup, `criteria_gaps` is **empty**, there is nothing new
  to say — do **not** draft-back on the criteria trigger. Fall through
  to the imbalance verdict (discuss/pass). (A matrix `draft-back`
  verdict still drafts back — see §6 step 3.) Re-drafting a PR with no
  new information is exactly the nagging this rule exists to prevent.
- Preserve any maintainer-authored text already in the marker block
  verbatim; the step only adds its own criteria lines.

---

## 7. Backtest mode

Triggered by "backtest the imbalance step", "how many ready PRs
would this close", or similar. **Read-only — acts on nothing.**

1. Load the session cache
   `/tmp/pr-management-triage-cache-<repo-slug>.json`. If absent or
   its `fetched_prs` bundle is missing, tell the maintainer to run a
   normal triage sweep first (the backtest reuses that sweep's
   fetched data rather than re-paginating).
2. Select the candidate set: PRs currently carrying the
   `ready for maintainer review` label, plus any whose cached
   `action_taken`/`suggested_action` was `mark-ready`.
3. For each, run §2–§6. Intrinsics (paths, size, author, labels)
   come from the cache for free; the §4 standing queries run
   on-demand only for PRs that pass the §5 exemptions.
4. Print a rollup and a per-PR table. Change nothing.

```text
Review-imbalance backtest — <repo> — <N> ready-for-review PRs

  would flip to recommend-close  : K   (of N)
  would flip to draft-back        : D   (criteria gaps, not closable)
  would flip to discuss-first     : J
  unchanged (pass)                : N-K-D-J
  exempt (committer/engaged/…)    : E

┌── PR ──┬─ area(s) ───────┬─ cost ──┬─ standing ─┬─ gaps ─┬─ verdict ───────┐
│ #NNNNN │ scheduler-jobs  │ extreme │ none       │ 3      │ recommend-close │
│ #NNNNN │ core-api        │ high    │ established│ 2      │ draft-back      │
│ #NNNNN │ cli             │ moderate│ weak       │ 0      │ pass            │
│  …     │                 │         │            │        │                 │
└────────┴─────────────────┴─────────┴────────────┴────────┴─────────────────┘
```

The `gaps` column is the post-dedup blocking `criteria_gaps` count.

Use the backtest to calibrate: a close rate that looks too high
usually means an area is over-tagged in its `AGENTS.md`, or the
standing queries are under-counting an author's in-area history.
Adjust the `AGENTS.md` tiers or the §4 thresholds, not the matrix.

---

## 8. What this step does NOT do

- It does **not** run code review — merit/correctness of the diff
  is [`pr-management-code-review`](../../.claude/skills/pr-management-code-review/SKILL.md)'s
  job. Imbalance is purely about review *economics* and *standing*.
- It does **not** close/draft anything on its own — every
  disposition is maintainer-fired.
- It does **not** apply to areas without seeded `AGENTS.md`
  metadata as if they were critical — unknown areas fall back to
  `medium`, which almost never reaches a CLOSE verdict on its own.
