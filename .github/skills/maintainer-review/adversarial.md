<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Adversarial reviewers — second-read integration

Some maintainers run a **second LLM reviewer** alongside their
own reading and the in-skill review to catch blind spots one
model would miss. The skill supports integrating any such
reviewer that exposes itself as a slash command in the
maintainer's harness — the maintainer names the command at
invocation time and the skill works it into the per-PR loop.

The skill does not ship a dependency on any particular plugin.
If the maintainer has none configured, Step 5 of
[`review-flow.md`](review-flow.md) is a no-op.

---

## Why bother

Two LLM reviewers with different training data flag different
classes of mistakes. The cost is one extra slash-command turn;
the benefit is meaningful for upstream PRs that land in front
of thousands of contributors. Adversarial framing — *"prove this
PR is wrong"* rather than *"check this PR for issues"* — pushes
harder on auth, data-loss, and race-condition assumptions, which
is the right gate for code that ships.

---

## How the maintainer configures one

Pass the slash command to invoke as the `with-reviewer:`
selector:

```text
/maintainer-review with-reviewer:/some-plugin:adversarial-review
```

The skill stores that command for the session and proposes it
at Step 5 of [`review-flow.md`](review-flow.md) on every PR.

To skip the proposal entirely for a session, pass
`no-adversarial`. To re-enable on the next session, drop the
flag — `with-reviewer:` does not persist across sessions
(deliberately; reviewers and their command names change, and
implicit state across sessions is hostile to maintainers who
share a checkout).

If the maintainer wants the same reviewer to be the default
across sessions, they put the slash command under a "Review
preferences" heading in their agent-instructions file —
project-scope `AGENTS.md` is the agent-agnostic canonical
location; harness-specific files (`.claude/CLAUDE.md`,
`~/.claude/CLAUDE.md`) work too. The skill picks the command up
from there at session start. The skill does **not** auto-discover
plugins or scan installed extensions.

---

## The "assistant proposes, user fires" constraint

Slash commands cannot be invoked from the assistant side. They
are user-side commands provided by the harness; only the human
user — or a configured hook — can fire them.

This means the per-PR adversarial step is a two-turn dance:

1. **Assistant proposes** — at Step 5 of
   [`review-flow.md`](review-flow.md), the assistant writes the
   proposal text and pauses:

   > *I've drafted my findings for PR #N (1 major, 3 minor;
   > see above). Want a second read? Type
   > `<ADVERSARIAL_COMMAND>` and I'll wait for the result. Or
   > `[N]o` / `[Q]uit` to skip.*

   `<ADVERSARIAL_COMMAND>` is the slash command the maintainer
   passed via `with-reviewer:` (or pulled from their
   agent-instructions file). The assistant types it back
   **literally** so the maintainer can copy-paste it; it does
   not paraphrase.

2. **User fires** — the maintainer types the slash command. It
   runs in the conversation and emits its findings as a normal
   message.

3. **Assistant integrates** — on its next turn, the assistant
   reads the second reviewer's findings, deduplicates against
   its own list, marks each finding with its `source:
   primary | adversarial | both`, and continues from Step 6 of
   [`review-flow.md`](review-flow.md).

The assistant never *promises* to invoke the slash command
itself. *"Running the adversarial review now…"* is the wrong
phrasing — it implies the assistant is about to fire the
command. The right phrasing is *"Type `<ADVERSARIAL_COMMAND>`
and I'll wait."*

---

## Background mode for large diffs

If the second reviewer supports a background-run flag (most do
for large diffs), the maintainer can pass it as part of the
slash command. The skill's proposal becomes:

> *Diff is large (47 files, +1.2k −800). Suggest:
> `<ADVERSARIAL_COMMAND> --background` so it runs async. When
> it finishes, surface the output (whatever your reviewer's
> result-fetch command is — e.g. `/<plugin>:result`) and I'll
> wait.*

Once the user pastes the result back, the assistant integrates
as in step 3 above.

The skill does not assume any particular result-fetch command
exists. If the maintainer's reviewer doesn't support
background mode, drop the suggestion.

---

## What the integration looks like

After the second reviewer returns, the assistant produces a
**combined findings list**. Each finding is annotated with its
source:

```yaml
- file: providers/foo/src/airflow/providers/foo/hook.py
  line: 142
  rule_source: .github/instructions/code-review.instructions.md
  rule_id: "Imports inside function bodies"
  source: both              # ← primary AND adversarial flagged this
  severity: minor

- file: providers/foo/src/airflow/providers/foo/hook.py
  line: 89
  rule_source: adversarial (security)
  rule_id: "Unbounded recursion on user-supplied JSON"
  source: adversarial       # ← adversarial-only
  severity: blocking        # ← adversarial flagged a real bug the primary missed

- file: providers/foo/tests/unit/foo/test_hook.py
  line: 33
  rule_source: AGENTS.md ("Use spec/autospec when mocking")
  rule_id: "Unspec'd Mock"
  source: primary           # ← primary-only
  severity: minor
```

The disposition pick (Step 6) then weighs the **combined**
findings: a `blocking` from the second reviewer flips the
disposition the same way a `blocking` from the primary review
does.

---

## Conflict between reviewers

If the primary review says *"this is fine"* and the second
reviewer says *"this is broken"* (or vice versa), surface the
disagreement to the maintainer **explicitly**:

> *Reviewers disagree on file.py:N. Primary: no concern.
> Adversarial: potential race condition (quoted: "the lock is
> released before the assertion"). Want me to drill into it,
> or defer to your judgment?*

Do not silently pick one. Disagreements between two LLM
reviewers are exactly the moments where the human reviewer's
judgment is most valuable; suppressing them defeats the
purpose of running two reviewers.

---

## When no adversarial reviewer is configured

If the maintainer didn't pass `with-reviewer:` and there's no
"Review preferences" entry in their agent-instructions file,
the skill announces once at session start:

> *No adversarial reviewer configured. Reviews this session use
> only my own pass. Pass `with-reviewer:<command>` next time if
> you want a second read.*

…and skips Step 5 entirely. The session summary notes which
PRs went through with single-reviewer coverage so the
maintainer can decide whether to back-fill manually later.

---

## Hook-based automation

Some plugins ship a `Stop` hook that auto-runs a generic review
at the end of each model turn. If the maintainer has one of
those installed, **the per-PR Step 5 proposal still runs
explicitly**. The two are independent:

- The hook runs at end-of-turn — catches anything obvious in
  the working state of files (and may run a non-adversarial
  variant of the reviewer).
- The skill's Step 5 proposes the **adversarial** variant
  pointed at the specific PR diff, with adversarial framing
  (auth / data-loss / race-condition default questioning).

Don't conflate the two. The hook is a safety net at end-of-
turn; the per-PR adversarial step is the actual review tool.
