---
name: airflow-publish-changes
description: Prepare or carry out authorized publication of Apache Airflow changes, including remote checks, pre-push verification, PR creation, and tracking deferred work. Also use when planning these steps or managing contribution issues.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Publish Airflow changes

For a plan, describe the steps without executing publication. For authorized
publication, preserve existing approval and confirmation boundaries.
Use the sections below for remote setup, existing PR checks, publication and
issue tracking. Follow root [`AGENTS.md`](../../../AGENTS.md) for the applicable
test conventions and generated check commands; for contribution text, use
[Draft Airflow contribution text](../airflow-pr-draft-summary/SKILL.md).

## Git remote naming conventions

Airflow standardises on two git remote names, and the rest of this file, the
contributing docs, and the release docs all assume them:

- **`upstream`** — the canonical `apache/airflow` repository (fetch from here).
- **`origin`** — the contributor's fork of `apache/airflow` (push PR branches here).

Always push branches to `origin`. Never push directly to `upstream` (and never
push directly to `main` on either remote).

**Before running any remote-based command, run `git remote -v` and verify the
names match this convention.** If they do not — for example, the upstream remote
is called `apache`, or `origin` points at `apache/airflow` with the fork under a
different name like `fork` — **do not silently go along with the existing
names**. Surface the mismatch to the user and propose the exact rename commands
to bring the checkout in line with the convention, then ask the user to confirm
before running them. Examples:

- Upstream is named `apache`, fork is `origin` (common legacy layout):

  ```bash
  git remote rename apache upstream
  ```

- `origin` points at `apache/airflow` and the fork is named `fork` (release-manager
  / "cloned upstream directly" layout):

  ```bash
  git remote rename origin upstream
  git remote rename fork origin
  ```

- Upstream is missing entirely:

  ```bash
  git remote add upstream https://github.com/apache/airflow.git
  # or, for SSH:
  git remote add upstream git@github.com:apache/airflow.git
  ```

- Fork is missing entirely:

  ```bash
  gh repo fork apache/airflow --remote --remote-name origin
  ```

After any rename/add, re-run `git remote -v` to confirm the new state before
continuing with commands that assume `upstream` / `origin`.

If a doc, script, or command you're about to run uses the old `apache` name (or
any other variant), **translate it to the `upstream` convention** in what you
propose to the user, rather than perpetuating the old name. Flag the stale
documentation so it can be fixed in a follow-up.

## Before starting: check for an existing PR

Before working on an issue, check for open PRs already addressing it
(`gh pr list --search "<issue number or keywords>"`, and look for `closes:`
/ `fixes:` references). Airflow allows parallel work — "better PR wins"
(see `contributing-docs/04_how_to_contribute.rst`) — but it is not the
default: prefer reviewing and building on an existing PR. Open a separate
one only *if your approach is genuinely different*. Do not blindly open
another near-identical PR for an issue that already has one (or several) —
that just adds reviewer noise.

## Creating Pull Requests

**Always push to the user's fork (`origin`)**, not to `upstream` (`apache/airflow`).
Never push directly to `main`.

Before pushing, confirm the remote setup matches the conventions above
(`upstream` → `apache/airflow`, `origin` → your fork). Run `git remote -v` and,
if the names don't match, propose renames as described in "Git remote naming
conventions" — ask the user to confirm before running them.

If the fork remote does not exist at all, create one:

```bash
gh repo fork apache/airflow --remote --remote-name origin
```

Before pushing, perform a self-review of your changes following the Gen-AI review guidelines
in [`contributing-docs/05_pull_requests.rst`](../../../contributing-docs/05_pull_requests.rst) and the
code review checklist in [`.github/instructions/code-review.instructions.md`](../../../.github/instructions/code-review.instructions.md):

1. Review the full diff (`git diff main...HEAD`) and verify every change is intentional and
   related to the task — remove any unrelated changes.
2. Read `.github/instructions/code-review.instructions.md` and check your diff against every
   rule — architecture boundaries, database correctness, code quality, testing requirements,
   API correctness, and AI-generated code signals. Fix any violations before pushing.
3. Confirm the code follows the project's coding standards and architecture boundaries
   described in root [`AGENTS.md`](../../../AGENTS.md).
4. Run regular (fast) static checks (`prek run --from-ref <target_branch> --stage pre-commit`)
   and fix any failures. This includes mypy checks for non-provider projects (airflow-core, task-sdk, airflow-ctl, dev, scripts, devel-common).
5. Run manual (slower) checks
   (`prek run --from-ref <target_branch> --stage manual --skip compile-ui-assets-dev --skip view-skill-eval --skip run-skill-eval-codex`)
   and fix any failures. The skipped hooks start long-running local servers or provision the
   opt-in Codex environment rather than run checks that complete.
6. Run relevant individual tests and confirm they pass.
7. Find which tests to run for the changes with selective-checks and run those tests in parallel to confirm they pass and check for CI-specific issues.
8. Check for security issues — no secrets, no injection vulnerabilities, no unsafe patterns.

Before pushing, always rebase your branch onto the latest target branch (usually `main`)
to avoid merge conflicts and ensure CI runs against up-to-date code:

```bash
git fetch upstream <target_branch>
git rebase upstream/<target_branch>
```

If there are conflicts, resolve them and continue the rebase. If the rebase is too complex,
ask the user for guidance.

Then push the branch to your fork (`origin`) and open the PR creation page in the browser
with the body pre-filled (including the generative AI disclosure already checked):

```bash
git push -u origin <branch-name>
gh pr create --web --title "Short title (under 70 chars)" --body "$(cat <<'EOF'
Brief description of the changes.

closes: #ISSUE  (if applicable)

---

##### Was generative AI tooling used to co-author this PR?

- [X] Yes — <Agent Name and Version>

Generated-by: <Agent Name and Version> following [the guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions)

EOF
)"
```

The `--web` flag opens the browser so the user can review and submit. The `--body` flag
pre-fills the PR template with the generative AI disclosure already completed.

Remind the user to:

1. Review the PR title — keep it short (under 70 chars), in the imperative mood, and focused on user impact. Do not use Conventional Commits prefixes (`fix:`, `feat:`, `chore:`, …).
2. Add a brief description of the changes at the top of the body.
3. Reference related issues when applicable (`closes: #ISSUE` or `related: #ISSUE`).

## Golden rule: when a fix is imminent, open the PR, not an issue

If you already know how to fix the problem and you (or the user) are going to
open the PR shortly, **do not file a GitHub issue first**. Go straight to the
PR.

- Airflow does not use issues as a changelog, as a parallel bug database, or
  as a duplicate record of in-flight work. The PR itself is the canonical
  record — title, description, diff, discussion, and merge all live in one
  place. An issue that gets closed by a PR a day later is double accounting
  that carries no information the PR does not already carry.
- Open issues attract drive-by submissions, often from other agents, that
  haven't seen the in-flight work. That produces duplicate fixes, low-quality
  PRs that have to be closed, and wasted reviewer time. Not opening the
  issue avoids creating that bait in the first place.
- If you catch yourself drafting an issue body that reads like the PR
  description you are about to write, that is the signal — skip the issue
  and open the PR.

The one exception is the case covered by the next section: the PR ships a
**workaround, mitigation, or partial fix** and the real follow-up work is
genuinely deferred to a later PR. There, the issue captures work that will
outlive the PR, so the issue is load-bearing rather than duplicate.

## Tracking issues for deferred work

When a PR applies a **workaround, version cap, mitigation, or partial fix**
rather than solving the underlying problem (for example: upper-binding a
dependency to avoid a breaking upstream release, disabling a feature
behind a flag, reverting a change that needs a better replacement, or
papering over a bug so a release can ship), the deferred work must be
captured in a GitHub tracking issue **and** the tracking issue URL must
appear as a comment at the workaround site in the code.

1. **Open the tracking issue first**, before finalising the PR body.
2. **Reference it in the PR body by number** — e.g. "full migration is
   tracked in #65609" — so anyone reviewing the PR can see what was
   deferred and why.
3. **Add a link to the tracking issue as a comment at the workaround
   itself**, so the reference survives after the PR merges and anyone
   reading the source later can click straight through to the follow-up
   work. Use the **full issue URL**, not bare `#NNNNN` — bare references
   do not auto-link outside GitHub's web UI (e.g. when grepping in an
   editor, browsing a checkout, or reading the file in a terminal).
   For example:

   ```toml
   # pyproject.toml
   # Remove the <1.0 cap after migrating to httpx 1.x;
   # tracked at https://github.com/apache/airflow/issues/65609
   "httpx>=0.27.0,<1.0",
   ```

   ```python
   # some_module.py
   # Delete this fallback once the new client is on all workers;
   # tracked at https://github.com/apache/airflow/issues/65609
   if old_client:
       ...
   ```

4. **Do not** write vague forward-looking phrases like "will open a
   tracking issue" or "to be filed later" in the PR body or in code
   comments. Open the issue, link it in both places, then submit the PR.
5. The tracking issue should describe: what the workaround is, why it
   was chosen, the concrete follow-up work needed, and any acceptance
   criteria for removing the workaround.

If a PR you already opened has such forward-looking language, open the
tracking issue, add a PR comment referencing the issue URL, and push a
follow-up commit that adds the tracking-issue URL as a comment at the
workaround site in the code.
