<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Adversarial reviewers — second-read integration

Some maintainers run a **second AI reviewer** alongside Claude
to catch blind spots one model would miss. Today this skill
explicitly knows about one: the OpenAI Codex plugin
(`codex@openai-codex`). The same pattern works for any other
slash-command-driven reviewer the user installs and surfaces in
their CLAUDE.md / memory.

---

## Why bother

Two AI reviewers with different training data catch different
classes of mistakes. The cost is one extra slash-command turn;
the benefit is meaningful for upstream PRs that land under the
maintainer's name in front of thousands of contributors.

The Codex memory note that motivates this for PR *submissions*
(in `airflow-s` projects) phrases it as:

> *Two AI reviewers with different training catch different
> blind spots. […] Adversarial mode pushes harder on auth, data
> loss, and race conditions, which is the right gate for both
> surfaces; the few extra seconds beat lighter `/codex:review`
> even on "obvious" changes.*

The same argument applies to **incoming** PRs the maintainer is
reviewing. The skill therefore proposes an adversarial pass per
PR by default, unless the maintainer disables it with the
`no-adversarial` selector.

---

## The "assistant proposes, user fires" constraint

`/codex:` slash commands (and most plugin-supplied slash
commands) are built into the Claude Code harness once the plugin
is installed. **The assistant cannot invoke them via Bash or any
tool.** Only the human user — or a configured hook (e.g. the
`Stop` review-gate hook the Codex plugin ships) — can fire
them.

This means the per-PR adversarial step is necessarily a
two-turn dance:

1. **Assistant proposes** — at Step 5 of
   [`review-flow.md`](review-flow.md), the assistant writes the
   proposal text and pauses:

   > *I've drafted my findings for PR #N (1 major, 3 minor;
   > see above). Want a second read? Type
   > `/codex:adversarial-review` and I'll wait for the result.
   > Or `[N]o` / `[Q]uit` to skip.*

2. **User fires** — the user types
   `/codex:adversarial-review`. The plugin runs in the
   conversation and emits its findings back as a normal message.

3. **Assistant integrates** — the assistant, on its next turn,
   reads Codex's findings, deduplicates against its own list,
   marks each finding's `source: claude | codex | both`, and
   continues from Step 6 of [`review-flow.md`](review-flow.md).

The assistant never *promises* to invoke Codex itself.
"Running `/codex:adversarial-review` now…" is the wrong
phrasing — it implies the assistant is about to fire the
slash command. The right phrasing is *"Run
`/codex:adversarial-review` and I'll wait."*

---

## Background mode for large diffs

For PRs with hefty diffs, Codex's review may take long enough
that synchronous wait is uncomfortable. The plugin supports
`--background`:

```text
/codex:adversarial-review --background
```

The assistant proposal becomes:

> *Diff is large (47 files, +1.2k −800). Suggest:
> `/codex:adversarial-review --background` so it runs async.
> When it finishes, run `/codex:result` to surface the output;
> I'll wait.*

Once the user pastes the result back, the assistant integrates
as in step 3 above.

---

## What the integration looks like

After Codex returns, the assistant produces a **combined
findings list**. Each finding is annotated with its source:

```yaml
- file: providers/foo/src/airflow/providers/foo/hook.py
  line: 142
  rule_source: .github/instructions/code-review.instructions.md
  rule_id: "Imports inside function bodies"
  source: both          # ← claude AND codex flagged this
  severity: minor

- file: providers/foo/src/airflow/providers/foo/hook.py
  line: 89
  rule_source: codex (security)
  rule_id: "Unbounded recursion on user-supplied JSON"
  source: codex         # ← codex-only
  severity: blocking    # ← Codex flagged a real bug Claude missed

- file: providers/foo/tests/unit/foo/test_hook.py
  line: 33
  rule_source: AGENTS.md ("Use spec/autospec when mocking")
  rule_id: "Unspec'd Mock"
  source: claude        # ← claude-only
  severity: minor
```

The disposition pick (Step 6) then weighs the **combined**
findings: a `blocking` from Codex flips the disposition the
same way a `blocking` from Claude does.

---

## Conflict between reviewers

If Claude says *"this is fine"* and Codex says *"this is
broken"* (or vice versa), surface the disagreement to the
maintainer **explicitly**:

> *Reviewers disagree on file.py:N. Claude: no concern. Codex:
> potential race condition (quoted: "the lock is released
> before the assertion"). Want me to drill into it, or
> defer to your judgment?*

Do not silently pick one. Disagreements between two AI
reviewers are exactly the moments where the human reviewer's
judgment is most valuable; suppressing them defeats the
purpose of running two reviewers.

---

## When Codex is not configured

If `~/.claude/plugins/installed_plugins.json` does not include
`codex@openai-codex` (or whichever plugin the project's
CLAUDE.md / memory designates as the adversarial reviewer), the
skill announces once at session start:

> *No adversarial reviewer configured. Reviews this session use
> only my own pass.*

…and skips Step 5 entirely. The session summary notes which
PRs went through with single-reviewer coverage so the
maintainer can decide whether to back-fill manually later.

---

## Other adversarial reviewers (extension point)

If the project introduces a different second reviewer (e.g. a
custom slash command, a `Bash`-runnable script that emits the
same finding shape), the integration follows the same shape:

1. Detect at session start (file existence, plugin install
   record, env var, CLAUDE.md rule).
2. Document the invocation in the user's CLAUDE.md or a
   memory file with `How to apply:` lines for: trigger,
   default invocation, background mode, integration of output.
3. The skill picks up the rule and surfaces the new
   reviewer in the per-PR proposal at Step 5 with the same
   "assistant proposes, user fires" framing.

The skill does **not** auto-discover new adversarial
reviewers. If a maintainer adds one, they document it and
re-invoke the skill.

---

## Hook-based automation (the review gate)

The Codex plugin ships an optional `Stop` hook
(`scripts/stop-review-gate-hook.mjs`, 900s timeout) that
auto-runs `/codex:review` (the *non-*adversarial variant) at
the end of each Claude turn. It is OFF by default; toggle on
via `/codex:setup --enable-review-gate`.

If the gate is on, **the per-PR Step 5 proposal still
explicitly proposes `/codex:adversarial-review`**. The two are
independent:

- The gate runs `/codex:review` automatically at end-of-turn —
  catches anything obvious in the working state of files.
- The skill's Step 5 proposes `/codex:adversarial-review` — a
  more pointed pass on the specific PR diff, with adversarial
  framing (auth / data-loss / race-condition default
  questioning).

Don't conflate the two. The gate is a safety net; the per-PR
adversarial step is the actual review tool.
