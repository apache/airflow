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

If no AIP source is supplied in the arguments or conversation, ask the user for
an AIP URL or pasted content. If they decline, print this usage synopsis and stop.

## Mode Detection

Classify inputs in this order:

1. Content already pasted in the conversation is the AIP source.
2. An argument starting with `https://cwiki.apache.org/confluence/` is the AIP URL.
3. Arguments starting with `https://github.com/apache/airflow/pull/` are PR URLs.
4. Remaining arguments that resolve to existing local files are file paths.

Do not interpret unrecognized free-form text as a file path. Ask whether it is
the AIP content or a path that needs correcting.

- **PR URLs present → post-implementation mode**: generates verified recipe playbooks from actual code.
- **No PR URLs → pre-implementation mode**: generates speculative user stories to help AIP authors validate design.

PR URLs signal post mode and serve as **discovery hints** — starting points for finding the implementation in the codebase. They are not an exhaustive list of all PRs for the feature, and the skill must explore beyond the supplied PRs to find the full implementation.

## AIP Number

Extract the AIP number from the URL path (e.g., `AIP-76` from a URL containing `/aip-76` or `/AIP-76`). If the number cannot be determined from the URL or pasted content, ask the user.

## Output Path

Write the final playbook to `files/aip-{number}.md` where `{number}` is the
extracted AIP number, creating `files/` if needed. If the file already exists,
ask the user before overwriting.

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
- For every implementation area, identify the owning distribution from the
   nearest `pyproject.toml` and inspect that distribution's source, tests,
   examples, and documentation. Do not assume an AIP is implemented only in
   `airflow-core`; implementations can span `airflow-ctl`, `task-sdk`, providers,
   clients, and other workspace packages.

The PRs are a starting point, not a boundary. Code may have been implemented in other PRs, refactored since the PR merged, or spread across modules the PR didn't directly modify.

### Phase 3 — Analyze

Cross-reference AIP features against the **codebase** (not the PRs):

- Which AIP features are implemented (found in the current codebase)?
- Which AIP features are NOT implemented (proposed in AIP but absent from the codebase)?
- What patterns exist in tests and example Dags that demonstrate usage?

### Package and Version Detection

For each recipe, identify the distributions that users must install and any
runtime or configuration prerequisites:

- Read package names from the owning distribution's `pyproject.toml`; do not
   infer an install name from its repository directory.
- Derive runtime and configuration prerequisites from current documentation,
   source, and tests.
- Search current documentation and PR diffs for `versionadded::` or other
   explicit release notes. Use a minimum version only when repository evidence
   establishes it.
- If the implementation exists in the current checkout but no released version
   can be verified, record it as **unreleased on the current checkout** or
   **release unknown**. Do not infer availability from a package's current
   `__version__` or from the PR merge date.
- Ask the user for a target Airflow version when it affects which recipes are
   applicable; verify that target against the evidence above.

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

1. **Verified** — the complete pattern exists in the current codebase. Label it
   `Verified` and cite the repository-relative source, example, or test path and
   relevant symbol below the block.
2. **Adapted** — combines verified components in a new way (e.g., using a
   verified mapper with a different asset). Label it `Adapted` and cite every
   repository-relative source path and symbol used to construct it.
3. **Unverified** — no codebase evidence for this pattern. Use placeholders:

   ```python
   # TODO: Implement [description]
   # See: [reference or AIP section]
   ...
   ```

Label this tier `Unverified` and cite the AIP section or other source that
motivated it. PRs are discovery and historical context, not proof that behavior
still exists in the current checkout.

### Phase 6 — Validate

Before writing the playbook, verify all of the following:

- Every approved recipe is present, and no unapproved recipe was added.
- Every code block has exactly one tier and the required evidence citations.
- Imports and symbols used by Verified blocks, and the source components used
   by Adapted blocks, exist in the current checkout.
- Package names, minimum versions, and runtime prerequisites have repository
   evidence. Unknown release availability is stated explicitly.
- No template placeholders remain. TODO placeholders appear only in Unverified
   blocks.
- Every implemented or unimplemented claim agrees with the current checkout.

If any check fails, report the unresolved item and do not write the playbook.

### Phase 7 — Assemble

Combine the overview and recipes into a playbook following the template structure. Write to `files/aip-{number}.md`.

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

Ask the user for the proposed target Airflow version. Label it as a design
target, not a released or verified minimum version.

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

### Phase 6 — Validate

Before writing the document, verify all of the following:

- Every approved story is present, and no unapproved story was added.
- Every code block contains the `PROPOSED API — not yet implemented` marker.
- The document makes no claim that speculative APIs exist in the current
   checkout.
- Release availability is marked as proposed and not yet implemented.
- Each story contains specific questions covering ergonomics, edge cases,
   compatibility, and implementation feasibility.
- No template placeholders remain outside intentionally speculative code.

If any check fails, report the unresolved item and do not write the document.

### Phase 7 — Assemble

Combine the overview and user stories into a document following the template structure. Write to `files/aip-{number}.md`.

---

## Gotchas

- **Confluence pages often return partial or JavaScript-rendered content via WebFetch.** If the fetched AIP content looks incomplete (missing sections, garbled HTML), ask the user to paste the content. Don't generate from partial input.
- **PR diffs can show intermediate code that was later revised.** When multiple commits exist in a PR, prefer the final state of files (use `gh pr view` with `--json files` and read the current branch/merged code) over the raw diff, which may include since-reverted changes. More broadly, always prefer the current codebase state over PR diffs — PRs are discovery aids, not the source of truth.
- **AIP terminology drifts from implementation.** AIPs are written before (or during) implementation. Class names, parameter names, and module paths in the AIP frequently differ from what was actually merged. Always verify names against the codebase, not the AIP text.
- **Test files reveal use cases that examples miss.** Example Dags tend to show the happy path. Test files (especially parametrized tests) expose edge cases, error conditions, and alternative configurations that make better recipes.
- **An AIP feature listed as "implemented" in the AIP may not be in the PR.** AIPs track overall status, not per-PR scope. Cross-reference each feature against actual PR code, not the AIP's status section.
