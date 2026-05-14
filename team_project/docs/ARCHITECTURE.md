# Architecture — AI-Assisted DAG Failure Triage Plugin

## Overview

The plugin is structured as four independent layers. Each layer has a single
responsibility and communicates with adjacent layers through a stable interface.
This satisfies the SOLID Single-Responsibility Principle and makes each layer
independently testable, replaceable, and deployable.

```
Airflow task-instance logs
        │
        ▼
┌─────────────────────────┐
│  Log Normalization Layer │  Strips noise, emits FailureContext
└────────────┬────────────┘
             │ FailureContext
             ▼
┌─────────────────────────┐
│  Heuristic Classifier   │  Returns ranked candidates
└────────────┬────────────┘
             │ RankedCandidates
             ▼
┌─────────────────────────┐
│  LLM Summarization Layer│  Optional; skipped in local-only mode
└────────────┬────────────┘
             │ DiagnosisSummary
             ▼
┌─────────────────────────┐
│  Remediation KB + UI    │  Displays runbooks alongside candidates
└─────────────────────────┘
```

## Layer Descriptions

### Log Normalization Layer

**Responsibility:** Accept a raw Airflow task-instance log string and return a
structured `FailureContext` dataclass.

Strips ANSI escape codes, extracts the final exception type and message,
identifies the stack-trace depth, and records metadata (task ID, DAG ID,
try number, executor type). Downstream layers never touch raw log text.

### Heuristic Classifier Layer

**Responsibility:** Map a `FailureContext` to a ranked list of
`FailureCandidate` objects, each carrying a category label and a confidence
score.

Rules are defined as an ordered priority list (e.g., check for
`ModuleNotFoundError` before checking for `OperationalError`). The output is
always a **ranked list**, never a single forced label. This is a deliberate
design choice: when the top candidate's confidence is below a configurable
threshold, the engineer sees multiple plausible categories and applies judgment
rather than blindly following a single automated verdict.

The classifier is entirely local — no network calls, no external dependencies
beyond the standard library. It runs in local-only mode.

### LLM Summarization Layer

**Responsibility:** Accept the top-N ranked candidates and the normalized log
excerpt and return a `DiagnosisSummary` with a human-readable explanation and
suggested next steps.

Sits behind a `Summarizer` protocol so it can be swapped for a different model
or a stub without modifying the classifier or the UI surface. Disabled entirely
when `triage.local_only = true` is set in `airflow.cfg`.

### Remediation Knowledge Base and UI Surface

**Responsibility:** Map a failure-category label to a remediation runbook and
surface both the ranked candidates and the runbook to the on-call engineer.

The KB is a version-controlled YAML file. Adding or updating a runbook is a
content change, not a code change, so it can be merged without a full release
cycle. The UI surface is a read-only Airflow plugin view — it does not mutate
any Airflow database state.

## Design Decisions

### Why ranked candidates instead of a single label?

Failure triage in distributed systems is rarely unambiguous. A
`ConnectionResetError` can indicate a transient network blip, a misconfigured
connection pool, or a downstream service outage — all requiring different
remediation. Forcing a single label would either require an artificially high
confidence threshold (leaving most failures unlabelled) or would mislead the
engineer when the classifier is uncertain. Returning ranked candidates makes the
classifier's uncertainty explicit and keeps the engineer in the loop.

### Why a separate normalization layer?

Both the classifier and the LLM layer need clean, structured input. Duplicating
normalization logic inside each layer would couple them to raw log format
changes. A single normalization layer means a log-format change is a one-place
fix, and both downstream layers automatically benefit.

### Why local-only mode?

Many Airflow deployments run in environments with strict data-residency or
air-gap requirements. Making local-only mode a first-class configuration option
(not a fallback or a stripped-down build) ensures the plugin is adoptable in
those environments from day one.
