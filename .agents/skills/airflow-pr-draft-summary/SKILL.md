---
name: airflow-pr-draft-summary
description: Draft Apache Airflow commit messages, PR titles and descriptions, or GitHub comments and reviews. Also use for release-note decisions when preparing a contribution.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Draft Airflow contribution text

Use the sections below for the requested contribution text. For PR descriptions,
follow [the repository template](../../../.github/PULL_REQUEST_TEMPLATE.md), keep
titles under 70 characters, and complete the Gen-AI checkbox and `Generated-by:`
attribution when applicable. Report only checks actually executed and their outcomes.
For release-note decisions, also follow the newsfragment constraints in root
[`AGENTS.md`](../../../AGENTS.md#coding-standards).

When deciding whether an issue is needed or preparing authorized publication, use
[Publish Airflow changes](../airflow-publish-changes/SKILL.md). Return the requested
text for a drafting request; drafting alone does not authorize posting.

## Commit messages, PR titles and release notes

Write commit messages focused on user impact, not implementation details.

- **Good:** `Fix airflow dags test command failure without serialized Dags`
- **Good:** `UI: Fix Grid view not refreshing after task actions`
- **Good:** `Update foo function`
- **Bad:** `Initialize Dag bundles in CLI get_dag function`
- **Bad:** `Update foo function (#12345)`
- **Bad:** `fix(cli): dags test failure` — Airflow does not use Conventional Commits
  (`feat:`, `fix:`, `chore:` …). Write the subject as plain prose. A `commit-msg`
  prek hook (`check-no-conventional-commit-message`) rejects these, and CI checks
  every commit of the PR.

**Always run `prek install` before committing any code.** It installs the
`commit-msg` hook (in addition to `pre-commit`) so the Conventional Commits guard
runs locally; a clone that ran `prek install` before this hook existed must re-run
it to pick up the new hook type.

Use the **imperative mood** and a plain message — do **not** use Conventional Commits prefixes
(`fix:`, `feat:`, `chore:`, `docs:`, `refactor:`, …). apache/airflow does not follow that
convention. (Area tags the project already uses, like `UI:` / `API:` / `Helm:`, are fine;
Conventional-Commit `type:` tokens are not.) The same rule applies to PR titles.

Do not include an issue or PR number in the PR title. GitHub already appends
the actual PR number automatically when a PR is squash-merged (this is why
Airflow's git history is full of titles like `... (#71609)`). Manually
adding a number in the title duplicates that auto-added number, and readers
cannot tell whether the number in the title refers to an issue or a PR —
which is misleading in the commit history and changelog. Reference the
issue only in the PR description, not the title

The commit message **body** should describe **why** the change is made — the motivation and
context — and **never what** the change is. The diff already shows what changed; restating it in
prose adds noise.

For `airflow-core` (and `chart/`, `dev/mypy/`) **user-facing** changes, add a newsfragment in that distribution's `newsfragments/` directory. **Golden rule: only add a newsfragment when you are certain the change is visible to users; when in doubt, do not add one** — a maintainer will request one in review if it is needed. Build/release tooling, CI, packaging, internal refactors, and dev-only scripts are not user-facing and must not get a newsfragment:
`echo "Brief description" > airflow-core/newsfragments/{PR_NUMBER}.{bugfix|feature|improvement|doc|misc|significant}.rst`

**Do not add newsfragments for `providers/` or `airflow-ctl/`** — their release managers regenerate the changelog from `git log` and do not consume newsfragments. Update the changelog directly when needed: `providers/<provider>/docs/changelog.rst` (see `providers/AGENTS.md`) or `airflow-ctl/RELEASE_NOTES.rst`. Changes to `task-sdk/` use `airflow-core/newsfragments/` since task-sdk ships in airflow-core.

- NEVER add Co-Authored-By with yourself as co-author of the commit. Agents cannot be authors, humans can be, Agents are assistants.

## GitHub messages drafted by agents

Anything an agent drafts that ends up posted to GitHub on the user's
account — PR / issue comments, PR-level reviews, line-level review
comments, discussion replies — must end with an attribution footer.
The footer is required whether or not a human reviewed the draft
first; what changes between the two cases is the wording.

Place the footer on its own paragraph at the end of the message,
separated from the body by a blank line and a horizontal rule. Use
the same agent name string used in `Generated-by:` on PR bodies (for
example, `Claude Code (Opus 4.7)`).

- **Agent draft, posted without prior human review** (autonomous /
  routine work, scheduled triage, etc.):

  ```
  ---
  Drafted-by: <Agent Name and Version> (no human review before posting)
  ```

- **Agent draft, reviewed and approved by a human maintainer before
  posting:**

  ```
  ---
  Drafted-by: <Agent Name and Version>; reviewed by @<github-handle> before posting
  ```

  The `@<github-handle>` is the human who actually read the draft
  and said "post it as-is" (or similar). It is not the user the agent
  is "running on behalf of" if no review took place — that case is the
  first form, not this one.

This footer is in addition to, not a replacement for, any per-tool
disclosure rules (the PR body still keeps its own `Generated-by:`
block under the AI-disclosure checkbox; commit messages still follow
the no-self-as-co-author rule above). Do not skip the footer to
shorten a message — attribution applies regardless of message length.

### Do not tag individuals

AI agents MUST NOT mention or tag individual contributors, committers,
PMC members, or maintainers using GitHub usernames (e.g. `@user`) unless
explicitly instructed by a human reviewer. When suggesting who might be
relevant to a discussion, refer to roles, teams, code ownership
information, labels, or components instead of individuals. This keeps
notification noise down and avoids pulling people into threads they have
not chosen to join.

The only exceptions are mentions a human has explicitly authorized —
including the `@<github-handle>` in the `Drafted-by: … reviewed by
@<handle>` footer above, which names the reviewer who approved the
message — and replying within a thread to people already actively
participating in that same PR/issue discussion.
