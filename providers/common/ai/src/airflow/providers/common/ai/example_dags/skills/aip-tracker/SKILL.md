---
name: aip-tracker
description: Track Airflow Improvement Proposal (AIP) implementation progress by comparing Confluence specs against codebase evidence. Use when asked to assess, report on, or compare AIP status.
license: Apache-2.0
---
<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# AIP Progress Tracker Skill

Assess implementation progress of Airflow Improvement Proposals by comparing
what the Confluence spec promises against what the codebase delivers.

## When to Use This Skill

Use this skill when the task involves:

- Tracking AIP implementation status
- Comparing a spec against shipped code
- Producing a cross-AIP progress report

## Available Tools

You have access to these tools for gathering evidence:

- `fetch_confluence_page(page_id)` -- fetch an AIP spec from Confluence
- `search_github_prs(query)` -- search GitHub PRs/commits for an AIP
- `get_repo_file_tree(path_prefix)` -- list files under a directory in the repo

## Evidence Gathering Strategy

For each AIP, gather evidence from **all three sources** before assessing:

1. **Confluence spec** -- call `fetch_confluence_page` with the AIP's page ID.
   Extract: deliverables, phases, completion criteria, and status.
2. **GitHub PRs and commits** -- call `search_github_prs` with both the AIP
   number (e.g. "AIP-76") AND topic keywords (e.g. "asset partition").
   Deduplicate results.
3. **Codebase file tree** -- call `get_repo_file_tree` with the AIP's known
   directory prefixes. Count source files and test files.

## Deliverable Extraction

Extract deliverables from the specification's own structure. Use these
sources in priority order:

1. **Numbered completion criteria** (e.g. "Definition of Done", "Completion
   Criteria") -- each numbered item is one deliverable.
2. **Phase definitions** -- each bullet or item under a phase heading is one
   deliverable.
3. **Explicitly enumerated components** (classes, operators, API endpoints,
   CLI commands, UI features) listed in the spec.

**Critical:** Do NOT split a single spec item into multiple deliverables
(e.g. do not count individual files within one component as separate
deliverables). Do NOT merge multiple spec items into one. Use the spec's
own granularity -- if the spec says "partition mappers" as one item, that
is one deliverable, not five separate file-level deliverables.

## Assessment Rules

1. For each deliverable, you MUST cite specific evidence (a PR number,
   commit message, or file path from tool results). If no evidence exists,
   mark the deliverable as "not_started" or "unclear" with low confidence.
2. Always express progress as fractions: "8/12 deliverables shipped".
   NEVER convert to percentages. Do not write "73%" or "72%" or any
   percentage. The fraction form "X/Y shipped" is the ONLY acceptable
   format.
3. Do NOT invent blockers or risks not directly supported by evidence.
   Use a "Notes" field for genuine uncertainties only.
4. If codebase evidence shows shipped work NOT mentioned in the spec,
   list it as "beyond_spec" and flag that the Confluence page needs
   updating.
5. PR numbers must come from `search_github_prs` results. Never
   fabricate or guess PR numbers.
6. Do NOT characterize AIPs with vague editorializing like "near
   completion", "minimal blockers", "requires foundational work", or
   "Advanced maturity". State the fraction (e.g. "9/10 shipped,
   1 in progress") and let the reader draw conclusions.
7. Do NOT add information beyond what the tools return. If a tool
   returns no results for a query, say so -- do not fill the gap
   with assumptions.

## Output Format

Structure the report as follows. Do not add sections, emojis, or
formatting beyond this template:

```
# AIP Progress Report

## Summary
- N AIPs tracked
- Per-AIP: AIP-{N} {Title}: X/Y shipped

## Per-AIP Breakdown

### AIP-{N}: {Title}
Confluence status: {status from spec}
Progress: X/Y deliverables shipped
Confluence update needed: {Yes/No}

**Shipped ({count}):**
  - {deliverable}: {evidence -- PR number or file path} [{confidence}]

**In progress ({count}):**
  - {deliverable}: PR #{number} [{confidence}]

**Not started ({count}):**
  - {deliverable}

**Beyond spec ({count}):**
  - {deliverable}: {evidence} [{confidence}]

**Notes:** {genuine uncertainties only, or omit if none}

## Cross-AIP Dependencies
- {only if tool evidence shows shared PRs or dependency chains}

## Confluence Updates Needed
- {list AIPs where spec is stale and what needs updating}
```

## Self-Verification (MANDATORY before returning the report)

Before returning the final report, re-read it and verify each of
these rules. If any check fails, fix the report before returning it.

1. Every deliverable cites evidence from tool results, or is explicitly
   marked "unclear" or "not_started".
2. No percentages appear anywhere in the report -- only "X/Y shipped"
   fractions.
3. Every PR number mentioned appears in `search_github_prs` results.
   Remove any that do not.
4. No invented blockers, risks, or characterizations beyond the
   evidence.
5. Beyond-spec items are flagged for Confluence update.
6. Deliverable granularity matches the spec's own structure -- no
   file-level splitting of spec-level items.
7. No vague editorializing ("near completion", "Advanced maturity",
   "substantially implemented"). Replace with the fraction.
8. shipped_count + in_progress_count + not_started_count +
   beyond_spec_count = total_count for each AIP.
