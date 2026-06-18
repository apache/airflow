<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

# agents — the agent-target registry (where framework-skill symlinks land)

Framework skills are **vendor-neutral content**: every supported
agent reads the *same* `SKILL.md` (the open Agent Skills format —
plain Markdown + a small YAML frontmatter). The skill body is
byte-identical no matter which agent loads it; there is **no
per-agent compile, adapter, or content transform**. The only thing
that genuinely differs between agents is **where on disk each one
looks for skills**. This file is the registry of those locations —
the single source of truth `adopt`, `upgrade`, `verify`,
`unadopt`, and `worktree-init` consult to decide *which directories*
to wire, refresh, health-check, and tear down.

It is the magpie analogue of a package manager's per-agent path
table: keep all vendor-specific knowledge here as *"where files
go"*, never as *"what files contain"*.

## The registry

| Target id | Project skills dir | Kind | Reads it |
|---|---|---|---|
| `universal` | `.agents/skills/` | universal **(canonical)** | Codex, Cursor, Gemini CLI, GitHub Copilot, OpenCode, Cline, Zed, Warp, Amp, and the rest of the cluster that converged on the shared path |
| `claude-code` | `.claude/skills/` | native (relay) | Claude Code |
| `github` | `.github/skills/` | native (relay) | GitHub's skill loader |
| `windsurf` | `.windsurf/skills/` | native (relay) | Windsurf |
| `goose` | `.goose/skills/` | native (relay) | Goose |

The table is **extensible**: a new agent that wants framework
skills is one new row (`id`, project dir, kind), nothing else —
the same way a path-registry-driven installer adds an agent. Do
not invent per-agent *content*; if an agent needs a different
directory, add a row, never a forked skill.

## The canonical directory — `.agents/skills/`

`.agents/skills/` is the **one canonical home** for every framework
skill. Its `magpie-<skill>` entries are the links that resolve to
the actual skill source — the gitignored snapshot
(`.apache-magpie/skills/<skill>/`) for a normal adopter, or the
in-repo `../../skills/<skill>/` source for the framework's own
[local self-adoption](adopt.md#local-self-adoption-methodlocal).

This is the load-bearing move for neutrality on two fronts:

1. **One placement covers the whole shared-path cluster.** A large
   set of agents (Codex, Cursor, Gemini CLI, GitHub Copilot,
   OpenCode, Cline, Zed, Warp, …) all read `.agents/skills/` as
   their project-scope skills path, so a single
   `.agents/skills/magpie-<skill>` link is seen by all of them:

   ```text
   .agents/skills/magpie-pr-management-triage   →  the canonical link
      ├─ Codex      picks it up
      ├─ Cursor     picks it up
      ├─ Gemini CLI picks it up
      └─ Copilot …  picks it up
   ```

2. **Every other target is a thin relay into it.** Agents with a
   bespoke folder (`claude-code` → `.claude/skills/`, `github` →
   `.github/skills/`, `windsurf`, `goose`, …) do **not** link into
   the snapshot independently. Each one gets a per-skill relay
   symlink that points back at the canonical entry:

   ```text
   .claude/skills/magpie-<skill>   →  ../../.agents/skills/magpie-<skill>
   .github/skills/magpie-<skill>   →  ../../.agents/skills/magpie-<skill>
   ```

   The snapshot path appears exactly **once** — in
   `.agents/skills/`. Re-pointing the framework at a new snapshot,
   or repairing a broken link, is a single-source operation; the
   relays follow automatically. Adopters keep their own native
   (non-`magpie-`) skills in `.claude/skills/` / `.github/skills/`
   untouched — only the `magpie-*` entries are relayed.

(Global / per-user skill paths diverge across agents — e.g.
`~/.cursor/skills/`, `~/.codex/skills/`, `~/.gemini/skills/`. The
framework's adoption is **project-scope** — it writes inside the
adopter repo — so it only ever cares about the project columns
above. Global installs are the operator's concern, out of scope
for `setup`.)

## Active-target selection — which dirs `adopt` wires

On every `adopt` / `upgrade` / `worktree-init`, the **active
target set** is computed as the union of:

1. **The always-on neutral targets** — `universal`
   (`.agents/skills/`, canonical) **plus** the `claude-code` +
   `github` relay pair. These three are wired unconditionally; the
   relays are cheap relative symlinks, harmless to an agent that
   never reads them, and dropping them is not a supported
   configuration.
2. **Any other registry target already present in the repo** —
   if `.windsurf/skills/` or `.goose/skills/` (etc.) already
   exists as a real directory, it is added to the active set so
   that agent sees the framework skills too (as a relay).
3. **Explicit opt-in** via the `agents:<list>` flag (see
   [`SKILL.md` Inputs](SKILL.md#inputs)) — a comma-separated list
   of registry ids. When passed it **replaces** the auto-detected
   set (1)+(2) for that run; `universal` is always retained even
   if omitted, because it is the canonical home every relay points
   at — dropping it would leave the relays dangling.

The flow **never** removes or rewrites an adopter's own
non-`magpie-` skill content in any target dir. It only adds /
repairs `magpie-*` symlinks. Whatever layout an adopter's
`.claude/` / `.github/` directories were in before, the framework
always wires the `magpie-*` set the same way: canonical in
`.agents/skills/`, relayed everywhere else.

## How the framework's rules generalise across targets

Every adoption rule is "canonical link, then relays", not
"per-target independent link":

- **`magpie-` prefix** ([`SKILL.md` Golden rule 6](SKILL.md#golden-rules))
  — unchanged. Every framework skill is `magpie-<skill>` in
  *every* active target dir, so it never collides with an
  adopter's own skills regardless of agent.
- **`.gitignore`** — one **uniform** block per active target dir,
  with no per-layout variation: `/<dir>/magpie-*` ignored plus
  `!/<dir>/magpie-setup` un-ignored. The negation keeps the one
  committed bootstrap (`magpie-setup`) tracked; the glob ignores
  the rest (the canonical links target the gitignored snapshot, so
  the relays that follow them dangle on a fresh clone). See
  [`adopt.md` Step 7](adopt.md#step-7--gitignore-entries-fresh-only).
- **Symlink wiring** — the canonical `magpie-<n>` →
  snapshot/source link is created once in `.agents/skills/`; every
  other active target (`claude-code`, `github`, `windsurf`,
  `goose`, …) gets a per-skill relay `magpie-<n>` →
  `../../.agents/skills/magpie-<n>`. See
  [`adopt.md` Step 8](adopt.md#step-8--wire-up-the-framework-skill-symlinks).
- **Committed bootstrap** ([`SKILL.md` Golden rule 6](SKILL.md#golden-rules))
  — the one committed framework artefact, `magpie-setup`, lives at
  the **canonical** `.agents/skills/magpie-setup/` (a committed
  copy for adopters; a committed symlink under self-adoption).
  `.claude`/`.github` carry a committed relay symlink to it.
- **Local self-adoption** (framework checkout) — canonical
  committed symlinks into `../../skills/<skill>/` in
  `.agents/skills/`, plus committed relays into
  `../../.agents/skills/magpie-<skill>` in every other active
  target. See
  [`adopt.md` → Local self-adoption](adopt.md#local-self-adoption-methodlocal).
- **`unadopt` / `worktree-init`** — every active target dir is
  torn down / propagated uniformly. Removing only `.claude` +
  `.github` would orphan the canonical `.agents/skills/magpie-*`
  links; removing only `.agents` would leave every relay dangling.

## SKILL.md format portability

The same `SKILL.md` is valid in every target with no
per-agent edit:

| Frontmatter field | Cross-agent behaviour |
|---|---|
| `name`, `description` | Universal — discovery works everywhere. |
| `when_to_use` | Claude-family routing hint; other agents may ignore it → discovery still works off `description`, only routing precision degrades. |
| `argument-hint`, `capability` | magpie / Claude extensions; non-supporting agents silently ignore them. |
| `license` | Inert metadata. |

Unknown frontmatter is ignored by each agent (graceful
degradation), so there is **no compile step and no per-agent
file**. The gitignored snapshot stays the single source of truth;
`.agents/skills/` links into it, and every other target dir
resolves into it through the `.agents/skills/` relay.

## The Claude-Code-only layer (not wired for other targets)

Some of what `adopt` installs is **genuinely Claude-Code-specific
and is wired only when the `claude-code` target is active**:

- `.claude/settings.json` — the sandbox (`network.allowedDomains`
  allowlist, `filesystem.denyRead`), the MCP-tool permission
  allowlist, and the hooks. Schema:
  `claude-code-settings.json`.
- `.claude/settings.local.json` — per-machine sandbox-allowlist
  entries.
- The `setup-isolated-setup-*` skill family — sandbox / pinned-
  tools / hooks installer.

Other agents adopt the **skills** (the neutral content) **without**
this layer.

> **Security caveat — this layer is a control, not cosmetics.**
> For a security framework the sandbox is a *confidentiality
> control* (it blocks exfiltration of non-public vulnerability
> data and reading `~/`). Running a security-class skill on an
> agent that lacks an equivalent control is a **policy decision**,
> not graceful degradation. Adopting the skills onto a non-Claude
> agent is supported; *executing confidential workflows there*
> requires the project to either declare that agent unsupported
> for those workflows or provide an equivalent control. `adopt`
> itself only places files — it does not grant that approval.
