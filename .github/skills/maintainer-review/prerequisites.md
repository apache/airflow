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

## 2. Resolve adversarial-reviewer configuration (DEGRADES)

The skill does not auto-discover plugins or scan installed
extensions. Adversarial-reviewer integration is opt-in: the
maintainer names the slash command at invocation time, or
documents it in their agent-instructions file.

In priority order:

1. **`with-reviewer:<command>` selector** on the current
   invocation — wins over everything else; the maintainer is
   explicit.
2. **Project-scope `AGENTS.md`** at the repo root, if it has a
   `## Review preferences` (or equivalent) section that names
   a slash command.
3. **Harness-specific project file** (e.g. `.claude/CLAUDE.md`)
   under the working directory, same convention.
4. **User-scope harness file** (e.g. `~/.claude/CLAUDE.md`),
   same convention.

If a command is found, announce once at session start:

> *Adversarial reviewer configured: `<COMMAND>`. After my
> review of each PR I'll propose typing it so we get a
> second read.*

If none is found, announce:

> *No adversarial reviewer configured. Reviews this session
> use only my own pass. Pass `with-reviewer:<command>` next
> time if you want a second read.*

If the maintainer passed `no-adversarial` explicitly, skip the
per-PR proposal regardless of what's configured (still
announce: *"adversarial reviewer disabled for this session"*).

See [`adversarial.md`](adversarial.md) for the full integration
mechanics — including why the assistant proposes the slash
command but never fires it.

---

## 3. Resolve the selector and compute working set (DEGRADES)

Translate the selector from [`selectors.md`](selectors.md) into a
GraphQL query and fetch the working list. The default
selector is the **"my reviews"** union of five signals:

1. **Review-requested** — open PRs where review is requested
   from `<viewer>`.
2. **Touching files I've recently modified** — open PRs that
   change any file in the maintainer's "active set" (files
   from the maintainer's open PRs on `<repo>` and files the
   maintainer has authored commits to on the base branch in
   the past 30 days).
3. **Codeowner** — open PRs that touch any file
   `CODEOWNERS` assigns to `<viewer>` directly or via team.
4. **Mentioned** — open PRs whose body / comments / reviews /
   commit messages contain `@<viewer>`.
5. **Reviewed-before** — open PRs that already have a real
   `gh pr review` from `<viewer>` (any state). Triage comments
   are excluded — they live in `comments[]`, not `reviews[]`.

See [`selectors.md`](selectors.md) for each signal's exact
query and the available `*-only` / `no-*` selectors that
narrow the union.

The active-set, codeowner, and team-membership computations
run once at the start of the session and are cached for the
rest of the run. The whole resolution stays well under the
maintainer's GraphQL budget.

If the selector produces zero PRs, say so and exit:

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

## Browser-open availability (DEGRADES)

The skill prompts before opening each PR's files tab in the
maintainer's default browser (see Golden rule 11 in
[`SKILL.md`](SKILL.md)). The opener is `xdg-open` (Linux),
`open` (macOS), or `start` (Windows). At session start the
skill checks that at least one is on `$PATH`:

```bash
command -v xdg-open >/dev/null 2>&1 \
  || command -v open >/dev/null 2>&1 \
  || command -v start >/dev/null 2>&1 \
  || echo "missing"
```

If none is available (headless session, container with no
freedesktop tools), announce once and degrade — the prompt is
still asked, but on `[y]` the skill **prints the files-tab
URL** instead of trying to launch:

> *No browser opener (`xdg-open` / `open` / `start`) available
> — on `[y]` I'll print the files-tab URL for you to click
> manually.*

The PR URL is still always rendered per Golden rule 10, so
the maintainer can click it directly in any URL-aware
terminal at any time.

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
