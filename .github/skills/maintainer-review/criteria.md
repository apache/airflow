<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Review criteria — pointers to source

This file is a **navigation map** for the project's review
criteria. It does not restate the rules — those live in the
source files below and are the single source of truth. The
skill's review pass reads them at session start (and re-reads
the per-area `AGENTS.md` files as PRs route into different
trees) and quotes the **source rule verbatim** in any finding
it raises.

If you find yourself wanting to "summarise the rule" in this
file or in a finding body, **stop and link to the source line
or section instead**. Summaries drift; links don't.

---

## Source files

| File | What it covers |
|---|---|
| [`.github/instructions/code-review.instructions.md`](../../../.github/instructions/code-review.instructions.md) | The rule set every Apache Airflow PR is reviewed against (architecture / DB / quality / testing / API / UI / generated files / AI-generated-code signals / quality signals). |
| [`AGENTS.md`](../../../AGENTS.md) | Repo-wide AI/agent instructions (architecture boundaries, security model, coding standards, testing standards, commits & PR conventions). |
| [`registry/AGENTS.md`](../../../registry/AGENTS.md) | Registry-tree-specific rules. |
| [`dev/AGENTS.md`](../../../dev/AGENTS.md) | `dev/` scripts conventions. |
| [`dev/ide_setup/AGENTS.md`](../../../dev/ide_setup/AGENTS.md) | IDE bootstrap conventions. |
| [`providers/AGENTS.md`](../../../providers/AGENTS.md) | Provider-tree boundary, compat-layer, and provider-yaml expectations. |
| [`providers/elasticsearch/AGENTS.md`](../../../providers/elasticsearch/AGENTS.md) | Elasticsearch-specific rules. |
| [`providers/opensearch/AGENTS.md`](../../../providers/opensearch/AGENTS.md) | OpenSearch-specific rules. |
| [`airflow-core/docs/security/security_model.rst`](../../../airflow-core/docs/security/security_model.rst) | The documented security model — what *is* and *isn't* a vulnerability. |

The per-PR review flow re-runs `git ls-files` against the
touched paths to discover any other `AGENTS.md` not in this
table; see [`review-flow.md#area-specific-overlay`](review-flow.md).

---

## Categories — link out to the source section

The headings below mirror the section structure of the source
files; click through for the actual rule text.

### Architecture boundaries

[`code-review.instructions.md` § Architecture Boundaries](../../../.github/instructions/code-review.instructions.md#architecture-boundaries) ·
[`AGENTS.md` § Architecture Boundaries](../../../AGENTS.md#architecture-boundaries)

### Database / query correctness

[`code-review.instructions.md` § Database and Query Correctness](../../../.github/instructions/code-review.instructions.md#database-and-query-correctness)

### Code quality

[`code-review.instructions.md` § Code Quality Rules](../../../.github/instructions/code-review.instructions.md#code-quality-rules) ·
[`AGENTS.md` § Coding Standards](../../../AGENTS.md#coding-standards)

### Testing

[`code-review.instructions.md` § Testing Requirements](../../../.github/instructions/code-review.instructions.md#testing-requirements) ·
[`AGENTS.md` § Testing Standards](../../../AGENTS.md#testing-standards)

### API correctness

[`code-review.instructions.md` § API Correctness](../../../.github/instructions/code-review.instructions.md#api-correctness)

### UI (React/TypeScript)

[`code-review.instructions.md` § UI Code (React/TypeScript)](../../../.github/instructions/code-review.instructions.md#ui-code-reacttypescript)

### Generated files

[`code-review.instructions.md` § Generated Files](../../../.github/instructions/code-review.instructions.md#generated-files)

### AI-generated code signals

[`code-review.instructions.md` § AI-Generated Code Signals](../../../.github/instructions/code-review.instructions.md#ai-generated-code-signals)

### Quality signals to check

[`code-review.instructions.md` § Quality Signals to Check](../../../.github/instructions/code-review.instructions.md#quality-signals-to-check)

### Commits and PRs (newsfragments, commit messages, tracking issues)

[`AGENTS.md` § Commits and PRs](../../../AGENTS.md#commits-and-prs)

---

## Provider-specific signals

When a PR touches `providers/<name>/`, the skill reads (and
quotes from) the provider-tree files in addition to the
repo-wide ones:

- [`providers/AGENTS.md`](../../../providers/AGENTS.md) — the
  provider-boundary, compat-layer, and `provider.yaml`
  expectations apply.
- `providers/<name>/AGENTS.md` if present — provider-specific
  rules (e.g.
  [`providers/elasticsearch/AGENTS.md`](../../../providers/elasticsearch/AGENTS.md),
  [`providers/opensearch/AGENTS.md`](../../../providers/opensearch/AGENTS.md)).

If the provider's tree has no `AGENTS.md`, the repo-wide rules
are still in effect.

---

## Security model — calibration

Before flagging anything that looks security-flavoured, read
the documented security model at
[`airflow-core/docs/security/security_model.rst`](../../../airflow-core/docs/security/security_model.rst)
and the
[`AGENTS.md` § Security Model](../../../AGENTS.md#security-model)
calibration guide. The latter is short and tells you how to
distinguish:

1. an **actual vulnerability** that violates the documented
   model — flag as blocking,
2. a **known limitation** that's already documented as
   intentional — do not flag,
3. a **deployment-hardening opportunity** — belongs in
   deployment guidance, not as a code finding.

When the skill downgrades what looked like a finding because
the documented model permits it, the review body **quotes the
relevant model paragraph** so the contributor sees the
calibration explicitly. Don't paraphrase.

---

## Backports and version-specific PRs

Branch `vX-Y-test` PRs are backports of already-merged `main`
work. They aren't called out in the repo-wide files, so the
calibration is local to this skill:

- **Diff parity**: does this match what was merged on `main`?
- **Cherry-pick conflicts**: did the resolution introduce new
  changes that need scrutiny?
- **API/migration version markers**: backports should not
  introduce new Cadwyn version bumps; if they do, that's a
  finding (cite
  [`code-review.instructions.md` § API Correctness](../../../.github/instructions/code-review.instructions.md#api-correctness)).

For these PRs, prefer `COMMENT` over `REQUEST_CHANGES` unless
the cherry-pick has clearly drifted from the `main` change.

---

## Conflict between source rules

If the per-area `AGENTS.md` rules **conflict** with the
repo-wide ones (rare; usually a more specific override), the
more specific one wins — but the conflict is surfaced to the
maintainer for explicit acceptance during disposition pick
(see [`review-flow.md`](review-flow.md)).

---

## When in doubt — defer

If after reading the diff you're not sure whether something is
a finding or just a style preference, **do not flag it**.
Surface the uncertainty to the maintainer (one line:
*"Hmm — line N does X, which I'm not sure violates the rules;
flagging for your eye."*) and let them decide. The cost of an
over-zealous auto-finding is a contributor who feels
nitpicked; the cost of a missed nit is one round of
back-and-forth a maintainer can catch easily on their own
pass.
