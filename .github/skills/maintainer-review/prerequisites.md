<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Prerequisites — pre-flight checks

The skill performs three pre-flight checks before fetching any
PR. Failures of check 1 are a hard stop; checks 2 and 3 degrade
gracefully with a one-line warning each.

---

## 1. `gh` authentication and collaborator access (HARD STOP)

```bash
gh auth status
```

Required outcome: the active account is logged in and the
selected protocol works (the skill uses HTTPS GraphQL queries via
`gh api`; SSH-only setups are fine because the API path doesn't
go through SSH).

The active account must additionally be a **collaborator** on
the target repo (`apache/airflow` by default). Without
collaborator access, the eventual `gh pr review` mutation in
[`posting.md`](posting.md) returns:

```
HTTP 403: Resource not accessible by integration
```

…with no other indication. The skill probes for collaborator
status up-front via:

```bash
gh api "repos/<repo>/collaborators/$(gh api user --jq .login)" \
  --jq .permission 2>/dev/null
```

A response of `admin`, `maintain`, or `write` is sufficient.
`triage` or `read` is not enough to post reviews; the skill
warns and offers `dry-run` mode (which drafts but does not post).

If `gh auth status` fails entirely, surface it and ask the
maintainer to run `gh auth login`. Do not proceed.

---

## 2. Detect locally-configured adversarial reviewer (DEGRADES)

The skill checks for a second-reviewer plugin / configuration so
it can propose invoking it during the per-PR flow (see
[`adversarial.md`](adversarial.md)). Today the skill knows about
one explicitly:

### OpenAI Codex plugin (`codex@openai-codex`)

```bash
jq -e '.plugins["codex@openai-codex"]' \
  ~/.claude/plugins/installed_plugins.json >/dev/null 2>&1 \
  && echo "codex installed" \
  || echo "codex not installed"
```

If installed, announce once at session start:

> *Adversarial reviewer detected: `codex@openai-codex`. After
> my review of each PR I'll propose `/codex:adversarial-review`
> so we get a second read.*

If the maintainer passed `no-adversarial`, skip the per-PR
proposal entirely (still announce: *"adversarial reviewer
disabled for this session"*).

### Other / future reviewers

If the maintainer has documented their own adversarial reviewer
in **any of**:

- a project-scope memory file under
  `~/.claude/projects/-<repo-slug>/memory/feedback_*.md` whose
  body matches `/adversarial.review|second.reviewer/i`,
- a project-scope `CLAUDE.md` rule under the working directory's
  `.claude/CLAUDE.md` that mentions a slash-command-driven
  reviewer,
- the user-scope `~/.claude/CLAUDE.md` mentioning the same,

…surface the rule's `How to apply:` instruction once at session
start and follow it during the per-PR flow. Do not try to
auto-discover *new* adversarial reviewers; if the user installs
one the skill doesn't know about, the user names it explicitly
when invoking the skill (e.g. `maintainer-review with-reviewer:foo`).

---

## 3. Resolve the selector (DEGRADES)

Translate the selector from [`selectors.md`](selectors.md) into a
GraphQL query and fetch the working list. If the selector
produces zero PRs, say so and exit:

> *No PRs match `<selector>` on `<repo>`. Nothing to review.*

Do not silently widen the search ("…so I'll show you PRs from
last month instead"). If the maintainer wants a wider net, they
re-invoke with a different selector.

---

## CI precheck (per PR, not per session)

Before showing each PR's headline (Step 2 in `SKILL.md`), the
skill checks the PR's status-check rollup state. This is
already in the per-PR `gh pr view` payload — it does not require
a separate call. The state is one of:

- `SUCCESS` — proceed normally; `APPROVE` is on the table.
- `PENDING` — proceed but flag in the headline ("CI still
  running"); the maintainer may want to defer the approve and
  use `[S]kip-for-now`.
- `FAILURE` / `ERROR` — proceed but per Golden rule 8 in
  `SKILL.md`, `APPROVE` is off the table; downgrade to
  `COMMENT` or `REQUEST_CHANGES`.
- `EXPECTED` (workflow approval pending) — surface explicitly
  and recommend `/pr-triage pr:<N>` for the workflow-approval
  flow first; do not attempt to review the PR until CI has
  actually run.

---

## Repo override

If the maintainer passes `repo:<owner>/<name>`, all checks
target that repo. For repos that aren't `apache/airflow`, also
check that the conventional `area:*` labels exist (since
[`selectors.md`](selectors.md) supports `area:` filters):

```bash
gh label list --repo <repo> --search "area:" --limit 1
```

If no `area:` labels exist, warn:

> *No `area:*` labels on `<repo>`. The `area:` selector will
> match nothing here.*

…and proceed with the rest of the selector.
