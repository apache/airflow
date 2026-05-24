<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

# overrides — manage agentic overrides for framework skills

The agentic-overrides mechanism is the framework's answer to
"how does an adopter modify a framework skill's behaviour
without forking the framework". An override file lives at
`.apache-steward-overrides/<framework-skill>.md` in the
adopter repo (committed). The framework skill consults the
file at run-time **before** executing default behaviour and
applies the agent-readable instructions in it.

This sub-action helps the user manage those override files —
list them, scaffold a new one, or open an existing one.

The full *contract* (what an override file may contain, how
the framework skill applies it, the hard rules that bound the
mechanism) lives in
[`docs/setup/agentic-overrides.md`](../../../docs/setup/agentic-overrides.md)
in the framework. This file is the operational helper.

## Inputs

- `<framework-skill>` — required. The skill name to scaffold
  / open the override for (e.g. `pr-management-triage`).

## Step 0 — Pre-flight

1. The repo must be adopted (see [`verify.md`](verify.md)
   check 1 + check 5). If not, redirect to `/setup-steward
   adopt`.
2. The named `<framework-skill>` must exist in the snapshot at
   `<repo-root>/.apache-steward/.claude/skills/<framework-skill>/`.
   If not, name the typo and list the available framework
   skills.

## Step 1 — Resolve the override path

`<override-path>` =
`<repo-root>/.apache-steward-overrides/<framework-skill>.md`.

If `<override-path>` already exists, this is an *open*
operation: surface the file's current content, ask the user
what they want to change, walk through the edit. Same as if
they had opened the file in their editor — the agent is just
doing it agentically.

If `<override-path>` doesn't exist, this is a *scaffold*
operation: continue to Step 2.

## Step 2 — Scaffold a new override

Read the framework skill's structure to know what the
override might target — the skill's section headings, golden
rules, decision-table rows, etc. Surface these as candidate
override anchors.

Ask the user what they want to override:

- *"Skip Step N"* → invalidate a specific step.
- *"Replace Step N with: ..."* → replace a step's behaviour.
- *"Add a new step before/after Step N: ..."* → insert.
- *"Always do X regardless of the framework's classification"*
  → pre-empt the framework's decision logic.
- Free-form — the agent interprets at run-time.

Generate the override file with the user's instructions.
Use the canonical scaffold below.

## Override file scaffold

```markdown
<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

<!-- apache-steward agentic override
     Framework skill:    <framework-skill>
     Pinned to snapshot: see ../.apache-steward.lock for the SHA
                          this override was authored against.
     Applied by:         the framework skill at run-time, before
                          executing default behaviour. -->

# Overrides for `<framework-skill>`

## Why these overrides exist

(One paragraph explaining the local context. Why does this
adopter need to deviate from the framework's default? Future
maintainers — including the agent on a later run — read this
to know whether the override is still relevant.)

## Overrides

### Override 1 — <one-line headline>

(Free-form agent-readable instructions. The framework skill
applies these before running its default behaviour. Be
specific about which step / golden rule / decision-table row
the override targets.)

### Override 2 — <one-line headline>

(...)
```

## Step 3 — Surface the contract reminders

Whenever the skill scaffolds or opens an override file,
remind the user:

1. **Never modify the snapshot** at
   `<repo-root>/.apache-steward/`. Local mods go in this
   override file.
2. **If the override is widely useful, upstream it.** Open a
   PR against `apache/airflow-steward` implementing the change
   in the framework skill itself. The framework will then
   apply the change on every adopter's next
   `/setup-steward upgrade`, and this adopter's override
   becomes redundant — at which point the user deletes it.
3. **Re-anchor on framework upgrades.** The skill's
   [`upgrade.md`](upgrade.md) sub-action surfaces conflicts
   when a framework upgrade restructures a skill the user has
   an override for. Re-anchor when prompted.

## Failure modes

- **Snapshot missing** → redirect to `/setup-steward upgrade`.
- **Skill name typo** → list available skills, ask again.
- **The override target is on a framework skill that does
  not consult overrides** → the framework treats overrides
  as opt-in per skill (each skill that supports overrides
  documents this in its own `SKILL.md`). If the named
  skill doesn't yet support overrides, surface that and
  suggest opening a framework-side issue requesting the
  hook.
