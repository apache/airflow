# Project Plan — AI-Assisted DAG Failure Triage Plugin

## Outcome Metric

Reduce the **median time-to-first-diagnosis** on a curated Airflow failure
corpus — measured from the moment a DAG run transitions to `failed` to the
moment the on-call engineer identifies the root-cause category.

## Sprint Overview

| Sprint | Goal | Key Deliverables |
|--------|------|-----------------|
| 1 | Foundation | Plugin scaffold, CI wiring, local dev environment |
| 2 | Data layer | Log normalization pipeline, failure corpus, baseline metrics |
| 3 | Classifier | Heuristic classifier with INVEST-validated acceptance tests |
| 4 | LLM layer | LLM summarization integration, local-only mode |
| 5 | Knowledge base | Remediation KB, ranked-candidate UI surface |
| 6 | Hardening | End-to-end tests, performance profiling, docs, demo |

## Deliverables

### Plugin Scaffold
A minimal, installable Airflow plugin package with entry-point registration,
empty classifier and summarizer stubs, and a passing CI pipeline. Establishes
the SOLID single-responsibility boundary between layers from day one.

### Log Normalization
A preprocessing stage that strips ANSI codes, trims stack-trace noise, and
emits a structured `FailureContext` dataclass consumed by both the classifier
and the LLM layer. Keeps raw-log coupling out of higher layers.

### Heuristic Classifier
Rule-based first-pass classifier that maps `FailureContext` fields to ranked
failure-category candidates (e.g., `dependency_missing`, `connection_timeout`,
`oom`, `user_code_error`). Returns a ranked list, not a single label, so that
engineer judgment is preserved when confidence is low.

### LLM Summarization Layer
Optional second-pass layer that feeds the top classifier candidates and the
normalized log excerpt to a language model and returns a human-readable
diagnosis summary and suggested next steps. Isolated behind an interface so it
can be swapped or disabled without touching the classifier.

### Remediation Knowledge Base
A curated, version-controlled YAML knowledge base mapping failure categories to
remediation runbooks. Consumed by the UI surface to display actionable guidance
alongside the ranked candidates.

### Local-Only Execution Mode
A configuration flag (`triage.local_only = true`) that bypasses any external
LLM API call and runs the heuristic classifier only. Ensures the plugin is
usable in air-gapped or data-residency-constrained deployments.

## Frameworks Applied

| Framework | How We Apply It |
|-----------|----------------|
| **Scrum** | Two-week sprints, daily stand-ups, sprint reviews with Prof. Champion |
| **Working Backwards** | Outcome metric (time-to-first-diagnosis) defined before any code is written |
| **INVEST** | Each user story is Independent, Negotiable, Valuable, Estimable, Small, Testable before sprint commitment |
| **SOLID** | Each layer (normalization, classifier, summarizer, KB) is a separate class with a single responsibility and a stable interface |
| **TDD** | Acceptance tests written from INVEST stories before implementation; classifier rules driven by failure corpus examples |
| **Shift-Left** | Ruff, mypy, and ASYNC lint checks run on every commit; ASYNC110 class of violations caught before merge |
