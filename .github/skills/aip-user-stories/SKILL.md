---
name: aip-user-stories
description: Generate verified recipe playbooks from AIPs with PR implementations (post mode), or speculative user stories from AIPs without implementations (pre mode). Use when the user provides an AIP URL or AIP content, optionally with PR URLs and file paths.
when_to_use: Trigger when the user mentions aip user stories, aip playbook, aip recipes, generate recipes from AIP, user stories for AIP, AIP guide, AIP how-to.
allowed-tools: Bash(gh pr view *) Bash(gh pr diff *) Read WebFetch(domain:cwiki.apache.org) WebFetch(domain:github.com)
argument-hint: <AIP-URL-or-pasted-content> [<PR-URL>...] [<file>...]
license: Apache-2.0
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# AIP-to-User-Stories Playbook

## Invocation

```
/aip-user-stories <AIP-URL-or-pasted-content> [<PR-URL>...] [<file>...]
```

If no arguments: print this usage synopsis and stop.

## Mode Detection

An argument starting with `https://cwiki.apache.org/confluence/` is the AIP URL. Arguments starting with `https://github.com/apache/airflow/pull/` are PR URLs. Other arguments are local file paths.

- **PR URLs present → post-implementation mode**: generates verified recipe playbooks from actual code.
- **No PR URLs → pre-implementation mode**: generates speculative user stories to help AIP authors validate design.

PR URLs signal post mode and serve as **discovery hints** — starting points for finding the implementation in the codebase. They are not an exhaustive list of all PRs for the feature, and the skill must explore beyond the supplied PRs to find the full implementation.

## AIP Number

Extract the AIP number from the URL path (e.g., `AIP-76` from a URL containing `/aip-76` or `/AIP-76`). If the number cannot be determined from the URL or pasted content, ask the user.

## Output Path

Write the final playbook to `.claude/aip-{number}.md` where `{number}` is the extracted AIP number. If the file already exists, ask the user before overwriting.

---

## Post-Implementation Mode

Source of truth: the **actual implementation** in the codebase and PRs — not the AIP specification. When the AIP proposes APIs that differ from what was implemented, the playbook follows the implementation.

### Phase 1 — Parse

Separate arguments into:

- **AIP source**: a URL to fetch, or pasted content already in the conversation.
- **PR URLs**: GitHub pull request URLs (one or more).
- **File paths**: local files (example Dags, source files, tests).

AIP content can come from a URL (fetched via WebFetch) or pasted directly by the user — both are equally valid input paths. If a URL fetch returns empty or garbled content, tell the user and ask them to paste the AIP content instead.

At least one PR URL is required in post mode. If none are provided but the mode was forced, ask for PR URLs.

### Phase 2 — Fetch & Discover

Retrieve initial sources:

- AIP content (from URL or already pasted).
- PR diffs and metadata via `gh pr view <number> --json title,body,files` and `gh pr diff <number>`.
- Local files specified as arguments.

Then use the PRs as **discovery seeds**: identify which modules, packages, and files the PRs touch, and explore outward from there:

- Read the touched files in their current state (not just the diff) to understand the full implementation.
- Follow imports, base classes, and related modules to find connected implementation code.
- Search for related example Dags, test files, and documentation that may not appear in the PR diffs.
- Grep for key class names, function names, and configuration keys from the AIP to find implementation spread across files the PRs didn't touch.

The PRs are a starting point, not a boundary. Code may have been implemented in other PRs, refactored since the PR merged, or spread across modules the PR didn't directly modify.

### Phase 3 — Analyze

Cross-reference AIP features against the **codebase** (not the PRs):

- Which AIP features are implemented (found in the current codebase)?
- Which AIP features are NOT implemented (proposed in AIP but absent from the codebase)?
- What patterns exist in tests and example Dags that demonstrate usage?

### Version Detection

Search PR diffs for `versionadded::` or `.. versionadded::` directives. If found, use that version. If not found, ask the user for the target Airflow version.

### Phase 4 — Propose

Present a numbered list of recipe candidates, grouped by concept:

```
**[Concept Group Name]**
1. Recipe Title — one-sentence description of what the user accomplishes
2. Recipe Title — one-sentence description
...
```

Each recipe maps to one distinct use case — a specific problem the user solves with this feature. If two API classes serve the same use case, combine them. If one class serves multiple use cases, split them.

For AIP features not found in the implementation, list them separately under **Not Yet Implemented** and ask the user: include with placeholder code, or skip?

Wait for user approval before generating.

### Phase 5 — Generate

For each approved recipe, produce content following the template in `references/playbook-template.md`.

**Code block tiers:**

1. **Verified** — the pattern exists in the codebase (source, example Dags, or tests). Use the code directly. No markers.
2. **Adapted** — combines verified components in a new way (e.g., using a verified mapper with a different asset). Clean code, with a brief note below the block: *"Adapted from [source file path]"*.
3. **Unverified** — no codebase evidence for this pattern. Use placeholders:

   ```python
   # TODO: Implement [description]
   # See: [reference or AIP section]
   ...
   ```

Verification sources: source code under `airflow-core/src/`, example Dags, and test files.

### Phase 6 — Assemble

Combine the overview and recipes into a playbook following the template structure. Write to `.claude/aip-{number}.md`.

If the user chose to skip unimplemented AIP features during the Propose phase, add a brief "Not Yet Implemented" section at the end listing them with one-line descriptions. Omit this section if all features were covered.

---

## Pre-Implementation Mode

Source of truth: the **AIP specification** itself. No implementation exists to verify against.

If file paths are provided, warn that they will be ignored (no implementation to reference).

### Phase 1 — Parse

Extract the AIP source: a URL to fetch, or pasted content. File path arguments are ignored with a warning.

AIP content can come from a URL or pasted directly — both are equally valid.

```
URL-based:  /aip-user-stories https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-76
Paste-based: /aip-user-stories (then paste AIP content when prompted)
```

### Phase 2 — Fetch

Retrieve AIP content from URL or accept pasted content. If a URL fetch returns empty or garbled content, ask the user to paste the AIP content.

Ask the user for the target Airflow version (no PR to extract `versionadded` from).

### Phase 3 — Analyze

Extract from the AIP:

- Proposed features, APIs, and configuration options.
- Use cases described or implied.
- Code examples provided in the AIP itself.

No code verification — nothing is implemented yet.

### Phase 4 — Propose

Present a numbered list of user story candidates, grouped by concept:

```
**[Concept Group Name]**
1. Story Title — one-sentence description of the user goal
2. Story Title — one-sentence description
...
```

Wait for user approval before generating.

### Phase 5 — Generate

For each approved story, produce content following the template in `references/playbook-template.md` (pre-mode section).

ALL code blocks must be marked as speculative:

```python
# PROPOSED API — not yet implemented
```

Base speculative code on the AIP's own code examples and proposed API as closely as possible.

Each story must include **open design questions** that probe:

- **API ergonomics** — Is this easy to use correctly and hard to misuse?
- **Edge cases** — What happens with unusual inputs, empty partitions, or unexpected configurations?
- **Compatibility** — How does this interact with existing Airflow patterns (catchup, backfill, dynamic task mapping, sensors, XCom)?
- **Implementation feasibility** — What constraints or complexities has the AIP not addressed?

Questions must be specific to the story's use case. Generic questions ("what about error handling?") do not count.

### Phase 6 — Assemble

Combine the overview and user stories into a document following the template structure. Write to `.claude/aip-{number}.md`.

---

## Gotchas

- **Confluence pages often return partial or JavaScript-rendered content via WebFetch.** If the fetched AIP content looks incomplete (missing sections, garbled HTML), ask the user to paste the content. Don't generate from partial input.
- **PR diffs can show intermediate code that was later revised.** When multiple commits exist in a PR, prefer the final state of files (use `gh pr view` with `--json files` and read the current branch/merged code) over the raw diff, which may include since-reverted changes. More broadly, always prefer the current codebase state over PR diffs — PRs are discovery aids, not the source of truth.
- **AIP terminology drifts from implementation.** AIPs are written before (or during) implementation. Class names, parameter names, and module paths in the AIP frequently differ from what was actually merged. Always verify names against the codebase, not the AIP text.
- **Test files reveal use cases that examples miss.** Example Dags tend to show the happy path. Test files (especially parametrized tests) expose edge cases, error conditions, and alternative configurations that make better recipes.
- **An AIP feature listed as "implemented" in the AIP may not be in the PR.** AIPs track overall status, not per-PR scope. Cross-reference each feature against actual PR code, not the AIP's status section.
