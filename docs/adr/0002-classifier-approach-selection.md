# 2. Classifier approach selection

Date: 2026-06-07

Issue: [#30 — Architecture Decision Record: classifier approach selection](https://github.com/break-through-19/airflow/issues/30)

## Status

Accepted

## Context

The DAG failure triage plugin must map a failed task instance's log content to
one or more **failure categories** (for example, transient network errors,
resource exhaustion, user code defects, external dependency failures) so the
UI can surface ranked remediation runbooks from the knowledge base.

Constraints that shaped this decision:

- **Timeline:** three-person team, one-semester delivery; classifier must ship
  before the LLM summarization sprint.
- **Data residency:** many target deployments require a **local-only mode** that
  must not send task logs to an external API (`triage.local_only` in
  `airflow.cfg`; see [`team_project/docs/ARCHITECTURE.md`](../team_project/docs/ARCHITECTURE.md)).
- **Operator trust:** on-call engineers must retain final judgment; a single
  forced label would mislead when classification is ambiguous.
- **Testability:** acceptance tests and CI must verify classification without
  flaky network calls or non-deterministic model output.
- **Downstream coupling:** remediation entries in
  [`team_project/data/remediation_kb.yaml`](../team_project/data/remediation_kb.yaml)
  are keyed by category and regex **signatures** aligned with pattern-based
  matching.

The team evaluated three approaches before implementing
[`FailureClassifier`](../team_project/src/dag_triage/classifier.py).

## Alternatives considered

### Alternative A — Rule-based (heuristic) classifier

Apply ordered regex and keyword rules over normalized log text. Accumulate
weighted scores per category; return a **ranked list** of `(category,
confidence)` pairs. No network calls; stdlib only.

| Strengths | Weaknesses |
|-----------|------------|
| Deterministic, fast, and easy to unit test | Brittle for novel or compound failures not covered by rules |
| Runs fully air-gapped | Rules require ongoing curation from a failure corpus |
| Maps directly to KB signature fields | No semantic understanding of free-form log prose |
| Low operational cost (no tokens, no model hosting) | Confidence is heuristic, not calibrated probability |

### Alternative B — LLM-only classification

Send normalized log excerpt (and optional metadata) to a language model with a
prompt that asks for failure category, explanation, and next steps. Single API
call replaces both classifier and summarizer.

| Strengths | Weaknesses |
|-----------|------------|
| Handles unseen failure modes and messy logs | **Blocked in local-only mode** unless a self-hosted model is deployed |
| Rich natural-language output in one step | Non-deterministic; harder to regression-test |
| Less manual rule maintenance | Latency and cost on every failed task view |
| | Sends potentially sensitive log data to a third party by default |
| | Category labels may drift from KB keys without strict prompt/schema control |

### Alternative C — Hybrid: rule-based classifier + optional LLM layer

Use **Alternative A** as the primary, always-on classification path. Add a
**separate** LLM summarization layer (behind a `Summarizer` protocol) that
consumes ranked candidates plus log excerpt to produce a human-readable
diagnosis. Disable the LLM layer when `triage.local_only = true`; classifier
and KB lookup still run.

| Strengths | Weaknesses |
|-----------|------------|
| Delivers value in air-gapped installs on day one | Two subsystems to maintain (rules + prompts) |
| Classifier output is stable input for KB lookup and UI badges | Layers can disagree; UI must show both ranked rules and LLM text |
| LLM can be added or swapped without rewriting classifier tests | Slightly higher implementation surface than A alone |
| Matches layered architecture already documented for the plugin | |

## Decision

**Adopt Alternative C (hybrid):** a **local rule-based ranked classifier** as
the authoritative source of failure **categories**, plus an **optional LLM
summarization layer** for natural-language root-cause narrative when external
API use is permitted.

Concrete rules for the implementation:

1. **`FailureClassifier`** returns ranked `(category, confidence)` tuples,
   never a single mandatory label (implemented in
   [`classifier.py`](../team_project/src/dag_triage/classifier.py)).
2. Categories are drawn from a fixed enum aligned with the remediation KB:
   `TRANSIENT`, `DATA_QUALITY`, `RESOURCE`, `CODE`, `EXTERNAL_DEPENDENCY`.
3. **`RemediationKB.lookup()`** uses the top-ranked category (and optional log
   signature) to select runbook steps shown in the Task Instance triage panel.
4. **LLM summarization** is a downstream layer (Sprint 4 scope); it must not
   be required for category badges or checklist rendering.
5. When `triage.local_only = true`, only the classifier + KB path executes; the
   UI falls back to the heuristic root-cause string built from classifier
   output and the last ERROR line (see
   [`triage_service.py`](../team_project/src/dag_triage/triage_service.py)).

## Rationale

- **Alternative A alone** satisfies local-only and testability but defers the
  product vision of AI-assisted plain-English diagnosis; summarization would
  still be needed as a second project.
- **Alternative B alone** fails the local-only acceptance path and introduces
  unacceptable flake and cost for a panel that must load without blocking the
  log viewer.
- **Alternative C** is the smallest design that meets all constraints: ship a
  testable classifier now, wire remediation immediately, and add LLM narrative
  behind a feature flag without rewriting classification or KB contracts.

This decision aligns with the four-layer pipeline in
[`team_project/docs/ARCHITECTURE.md`](../team_project/docs/ARCHITECTURE.md)
(normalization → classifier → optional LLM → remediation + UI).

## Consequences

### Positive

- Classifier behavior is covered by deterministic unit tests
  ([`team_project/tests/test_classifier.py`](../team_project/tests/test_classifier.py)).
- Triage panel can show categories and checklists with zero external
  dependencies (issue #33).
- Future LLM work is isolated behind `Summarizer`; swapping models does not
  change KB or classifier interfaces.

### Trade-offs and known limitations

- **Rule coverage gaps:** failures that match no pattern return an empty
  category list; engineers rely on raw logs until rules or corpus entries are
  added.
- **Heuristic confidence:** scores are rule weights capped at 1.0, not
  statistically calibrated probabilities.
- **Layer disagreement:** LLM summary may emphasize a different cause than the
  top rule-based category; UI presents both rather than merging them silently.
- **Maintenance:** new Airflow operator ecosystems may require new rule packs;
  changes should be driven by the curated failure corpus (Sprint 2/6).

### Follow-up work

- Integrate classifier with `FailureContext` from the normalization layer
  (today classifier accepts raw log text).
- Add configurable confidence threshold for highlighting a single “primary”
  category in the UI.
- Measure time-to-first-diagnosis on the failure corpus against the outcome
  metric in [`team_project/docs/PROJECT_PLAN.md`](../team_project/docs/PROJECT_PLAN.md).

## Review checklist (issue #30)

| Criterion | Met |
|-----------|-----|
| ADR committed under `docs/adr/` | Yes |
| At least two alternatives documented | Yes — rule-based, LLM-only, hybrid |
| Decision, rationale, and trade-offs captured | Yes |
| Reviewed by all three team members | Yes — see below |

## Reviewers

| Reviewer | Role | Approval |
|----------|------|----------|
| Sharan Saravanan | Project scoping and engineering (ADR author, issue owner) | Approved |
| Poorani T S | Engineering and coordination | Approved |
| Sohail Anwar | Scrum Master | Approved |

Team approval is recorded through pull-request review on the branch that
closes #30. Update this table with GitHub handles if formal sign-off is
captured outside the PR thread.
