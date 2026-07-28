---
name: sql-reporting
description: Conventions and review steps for writing analytics SQL against the warehouse. Use whenever the task involves querying tables, building a report, or aggregating metrics.
license: Apache-2.0
---
<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# SQL Reporting Skill

Apply this skill before writing or running any analytics SQL so reports stay
consistent and safe.

## When to Use This Skill

Use this skill when the task involves:

- Querying warehouse tables for a metric, report, or dashboard figure
- Aggregating rows (counts, sums, rolling windows)
- Cross-referencing two or more tables

## Conventions

1. Always `SELECT` explicit column names, never `SELECT *`.
2. Filter on a partition/date column first to bound the scan.
3. Alias aggregates with snake_case names (`order_count`, not `count(*)`).
4. Cap exploratory queries with `LIMIT` unless an aggregate already collapses
   the result set.
5. Prefer `COUNT(DISTINCT ...)` over a sub-query when de-duplicating.

## Review Checklist (run before returning an answer)

- [ ] No `SELECT *`.
- [ ] A date or partition predicate is present.
- [ ] Every aggregate has an explicit alias.
- [ ] The query reads only from tables the task actually needs.

## Output Format

Return the final SQL in a fenced ```sql block, then one sentence describing
what the query returns.
