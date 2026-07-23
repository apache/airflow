---
triage_review_imbalance:
  area: docs
  criticality: low
  review_difficulty: low
  structural_risk_paths:         # generated content — a hand-edit here is always wrong
    - "migrations-ref.rst"
    - "security/api_permissions_ref.rst"
    - "installation/supported-versions.rst"
    - "redirects.txt"
  codeowners_ref: ".github/CODEOWNERS"
  experts: []                    # docs is broadly owned; no dedicated expert gate
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
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

## …and the two places where they are not

The low-cost framing holds for corrections, clarifications, links, and examples.
It does not hold for two kinds of change that the closed-PR record shows
consuming disproportionate review time:

- **New prose about a subsystem the author does not work on.** Airflow's
  behaviour is conditional almost everywhere — Dag versioning depends on bundle
  support and on versioning being enabled; `extra` masking depends on key names;
  template rendering coerces types under some settings. A page written from
  reading the source is locally plausible and globally wrong, and only the
  people who own the subsystem can tell. Reviewing such a page costs more than
  writing it did.
- **Edits to generated content.** Much of this tree is projected from the code
  and rewritten by a hook or a Sphinx extension. A hand-edit there looks like an
  improvement, disappears on the next run, and can fail CI on an unrelated
  contributor's PR. See [ADR 0001](adr/0001-reference-pages-are-generated-not-written.md).

## Conventions a contributor should follow

- Write **Dag** (title case) in prose; keep literal code tokens (`DAG`,
  `dag_id`, `airflow dags list`, …) as-is (see the repo `CLAUDE.md`).
- Build docs with `breeze build-docs` before proposing changes. The default run
  includes spellcheck; `--spellcheck-only` and `--docs-only` narrow it.
- Read the area's ADRs in [`adr/`](adr/) before a change that adds a page,
  moves one, or touches a reference page.

## Review criteria

Mined from real review discussion on the ~778 commits that have touched this
tree since 2024 and on 259 closed-unmerged PRs (95 of them with substantive
review discussion). **If you are preparing a docs change, apply this checklist
_before_ opening the PR.** Most items are cheap; docs PRs should rarely be
drafted back and more rarely closed. Ordered by how often reviewers raise each.

**Build it yourself:**

- [ ] **`breeze build-docs` passes locally**, including spellcheck — no broken
      build, no dead internal links, no unresolved cross-references. Intentional
      jargon that trips spellcheck goes in `docs/spelling_wordlist.txt` (which a
      prek hook keeps sorted), not into a `# noqa`-style suppression.
- [ ] **"I do not have Breeze set up" is not a workflow.** Asking a reviewer to
      build the docs, confirm a link renders, or re-run a job on your behalf is
      the point at which these PRs get closed. Get the environment working
      first; the contributor channels exist for that.
- [ ] **RST is whitespace-sensitive.** Directive bodies, indentation under
      titles, and blank lines around blocks are the most common build failures;
      they are fixed by pushing to the branch, not by opening a new PR.

**Accuracy over coverage (see [ADR 0002](adr/0002-documentation-ships-with-the-behaviour-it-describes.md)):**

- [ ] **Write what you verified.** Describe behaviour you reproduced or hit in
      practice, not behaviour inferred from reading the source.
- [ ] **State the conditions.** For anything conditional — versioning, bundles,
      masking, timetables, backfill — an unconditional sentence is a wrong
      sentence. Narrow the claim rather than generalising it.
- [ ] **Check with the owning area before adding a new page** about a subsystem
      you do not work on. Some documentation gaps are engineering tasks in
      disguise and need design and testing before anything can be written.
- [ ] **Do not restate the release notes or an existing page.** Link, and keep
      the explanation in one place.
- [ ] **Runnable Python is best placed in an example Dag**, pulled in with
      `exampleinclude` from `example_dags/`, so the sample is code the test
      suite runs — inline `code-block` samples are never executed and can rot
      silently. This is a _preference, not a violation_: the tree carries
      inline `code-block:: python` roughly 405 times against 69
      `exampleinclude` uses, so an inline sample is the house norm and is not
      grounds for a change request. See
      [ADR 0002](adr/0002-documentation-ships-with-the-behaviour-it-describes.md),
      which de-lists it explicitly.

**Generated content and page identity:**

- [ ] **Never hand-edit generated content** — the whole of
      `security/api_permissions_ref.rst`, everything between the
      `auto-generated table` markers in `migrations-ref.rst` and
      `installation/supported-versions.rst`, and the directive-rendered
      reference in `configurations-ref.rst`, `cli-and-env-variables-ref.rst`,
      `stable-rest-api-ref.rst`, `database-erd-ref.rst`, and
      `operators-and-hooks-ref.rst`. Fix the source and regenerate.
- [ ] **Regenerate in the same PR** when you change a config option, CLI flag,
      REST route, permission, or migration.
- [ ] **Moving, renaming, or deleting a page requires a `redirects.txt` entry.**
      Published documentation URLs are permanent; a silent move breaks external
      links and search results.
- [ ] **Version-aware links.** Point at the documentation namespace that
      actually hosts the target, and do not link to paths that only exist for
      unreleased versions.

**Scope, hygiene, and what gets closed:**

- [ ] **No vendor or third-party product how-tos** — integrations live in
      providers. No product names, company names, or external links inserted
      where a generic description or an internal link would serve. See
      [ADR 0003](adr/0003-the-documentation-is-not-a-promotion-surface.md).
- [ ] **Read what you (or your tools) wrote, end to end.** Duplicated
      paragraphs, sections restating each other, and prose the author cannot
      explain are the tell reviewers act on, and the fastest route to a closure.
- [ ] **Nothing unrelated in the diff.** Wrong branch, unrelated commits picked
      up from a bad rebase, and hundred-file diffs on a doc PR are closed rather
      than untangled.
- [ ] **Iterate in place.** Force-push the branch to address review feedback;
      do not open a successor PR. Maintainers correct this every time — the
      review threads do not travel.
- [ ] **Check for an existing PR first.** Several contributors regularly land on
      the same broken link or stale page. Note the asymmetry: a fix that already
      exists _on `main`_ makes a change redundant, whereas a competing _open_ PR
      does not. Airflow allows parallel work and the better PR wins (root
      `CLAUDE.md`; `contributing-docs/04_how_to_contribute.rst`), so an open
      duplicate is a triage comment — "see also #NNNN" — never a ground for
      closing.
- [ ] **PR title says _why_, not _what_**, in plain prose, and no
      Conventional-Commit prefix.
- [ ] **No newsfragment for a docs-only change** unless it is genuinely
      user-facing in its own right.

> Extend this list as new review patterns emerge, and add an equivalent
> `## Review criteria` section to the `AGENTS.md` of every other area over time.

### What these documents are currently good for

A validation pass ran this area's ADRs against the live open-PR queue and found
**no ADR-level true positives across 13 PRs** — all four firings were PR-title
nits, which the root `CLAUDE.md` already covers. That is recorded here
deliberately, as a finding rather than a gap: the ADRs in `adr/` currently
function as _review guidance and onboarding context_, not as a mechanical gate on
the queue. It fits what this area's own instructions say — docs PRs "should
rarely be drafted back and more rarely closed". Do not sharpen the rules until
they catch something; inventing a trigger would produce false positives against
merged work and destroy the signal this measurement carries.

## Expectation for large changes

No discuss-first requirement for corrections, clarifications, links, or
examples — the bulk of what lands here. Do agree the approach first, in an issue
or on the dev list, before: a **new page or section** documenting a subsystem
you do not work on; a **restructure** that moves or renames existing pages
(which also needs `redirects.txt` entries); and any change to how documentation
is **generated or built**.
