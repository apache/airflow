# 1. Record architecture decisions

Date: 2026-06-07

## Status

Accepted

## Context

The Milestone Mavericks team is delivering an AI-assisted DAG failure triage
plugin on a fork of Apache Airflow (`github.com/break-through-19/airflow`).
Multiple contributors will work across sprints on log ingestion, classification,
summarization, and UI layers. Design choices need a durable, reviewable record
so future work does not re-litigate settled decisions.

## Decision

We will use **Architecture Decision Records (ADRs)** stored in `docs/adr/` at
the repository root of our fork. Each ADR captures context, alternatives
considered, the decision, rationale, and known consequences.

ADRs are numbered sequentially (`0001`, `0002`, …) and indexed in
[`docs/adr/README.md`](README.md).

## Consequences

- Design rationale survives beyond individual PRs and sprint notes.
- New contributors can read ADRs before changing classifier, summarization, or UI
  boundaries.
- ADR review is part of the team's Definition of Done for architectural stories
  (for example, issue #30).

## Reviewers

| Reviewer | Role | Approval |
|----------|------|----------|
| Sharan Saravanan | Project scoping and engineering (author) | Approved |
| Poorani T S | Engineering and coordination | Approved |
| Sohail Anwar | Scrum Master | Approved |

Formal sign-off is recorded via pull-request approval on the ADR commit linked
to issue #30.
