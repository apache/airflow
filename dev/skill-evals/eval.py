#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# /// script
# requires-python = ">=3.9"
# ///
"""Airflow skill-eval harness.

Test whether AGENTS.md guidance affects agent decisions by comparing
arms with and without the guidance. Each arm is a git worktree of the
real repo — the agent sees actual source files.

Usage:
    uv run dev/skill-evals/eval.py                  Test AGENTS.md changes
    uv run dev/skill-evals/eval.py --full            Add baseline arm (no AGENTS.md)
    uv run dev/skill-evals/eval.py --repeat 3        Reduce nondeterminism

Authentication: Claude Code session (claude /login) or ANTHROPIC_API_KEY.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PROMPTFOO_VERSION = "0.121.17"

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
AGENTS_SRC = REPO_ROOT / "AGENTS.md"
CASES_DIR = SCRIPT_DIR / "cases"
HASH_FILE = SCRIPT_DIR / "last-eval-hash.txt"

# Shared with the check-eval-hash prek hook so recorded and verified hashes can't drift.
sys.path.insert(0, str(REPO_ROOT / "scripts" / "ci" / "prek"))
from check_eval_hash import compute_guidance_hash, write_recorded_hash  # noqa: E402

OUTPUT_SCHEMA = """\
      output_format:
        type: json_schema
        schema:
          type: object
          required: [should_create, rationale]
          additionalProperties: false
          properties:
            should_create:
              type: boolean
            type:
              type: string
              enum: [bugfix, feature, improvement, doc, misc, significant]
            rationale:
              type: string"""


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=False, **kwargs)


def check_prerequisites() -> None:
    if not shutil.which("node"):
        print("Error: Node.js not found. Install Node.js >=22.22.0", file=sys.stderr)
        sys.exit(1)
    if not shutil.which("npx"):
        print("Error: npx not found.", file=sys.stderr)
        sys.exit(1)

    sdk_dir = Path.home() / ".promptfoo-sdk" / "node_modules" / "@anthropic-ai" / "claude-agent-sdk"
    if not sdk_dir.is_dir():
        print("Error: Claude Agent SDK not found. Run:", file=sys.stderr)
        print(
            "  mkdir -p ~/.promptfoo-sdk && cd ~/.promptfoo-sdk"
            " && npm init -y && npm install @anthropic-ai/claude-agent-sdk",
            file=sys.stderr,
        )
        sys.exit(1)


def check_claude_md_symlink(base_branch: str) -> None:
    """Verify CLAUDE.md is a symlink to AGENTS.md, in the working tree and on the base branch.

    The Claude Agent SDK reads CLAUDE.md. Swapping AGENTS.md between arms only
    affects the agent while CLAUDE.md resolves to it — if CLAUDE.md ever becomes
    a regular file, every arm would read identical guidance and the eval would
    silently measure nothing.
    """
    link = REPO_ROOT / "CLAUDE.md"
    working_ok = link.is_symlink() and os.readlink(link) == "AGENTS.md"
    tree_entry = run(["git", "-C", str(REPO_ROOT), "ls-tree", base_branch, "CLAUDE.md"]).stdout
    target = run(["git", "-C", str(REPO_ROOT), "show", f"{base_branch}:CLAUDE.md"]).stdout
    branch_ok = tree_entry.startswith("120000") and target == "AGENTS.md"
    if not (working_ok and branch_ok):
        print(
            "Error: CLAUDE.md must be a symlink to AGENTS.md (in the working tree"
            f" and on '{base_branch}'). The eval swaps AGENTS.md between arms and"
            " relies on the agent reading it through that symlink.",
            file=sys.stderr,
        )
        sys.exit(1)


def resolve_base_branch() -> str:
    result = run(["git", "-C", str(REPO_ROOT), "rev-parse", "--verify", "main"])
    if result.returncode == 0:
        return "main"
    branch = run(["git", "-C", str(REPO_ROOT), "rev-parse", "--abbrev-ref", "HEAD"]).stdout.strip()
    print(f"Warning: 'main' not found, using current branch '{branch}' as base")
    return branch


def git_show_file(base_branch: str, path: str, dest: Path) -> bool:
    result = run(["git", "-C", str(REPO_ROOT), "show", f"{base_branch}:{path}"])
    if result.returncode != 0:
        return False
    dest.write_text(result.stdout)
    return True


def create_worktree(
    work_dir: Path,
    name: str,
    base_branch: str,
    agents_file: Path | None,
    worktrees: list[Path],
    skill_name: str | None = None,
    skill_file: Path | None = None,
) -> Path:
    """Create a git worktree arm with the specified AGENTS.md and optional SKILL.md."""
    wt_dir = work_dir / name
    result = run(
        [
            "git",
            "-C",
            str(REPO_ROOT),
            "worktree",
            "add",
            "--quiet",
            "--detach",
            str(wt_dir),
            base_branch,
        ]
    )
    if result.returncode != 0:
        print(
            f"Error: 'git worktree add' failed for arm '{name}':\n{result.stderr.strip()}",
            file=sys.stderr,
        )
        sys.exit(1)
    # Register before mutating the checkout so cleanup covers this worktree
    # even if a later step in this function fails.
    worktrees.append(wt_dir)

    if agents_file is None:
        # Baseline arm: remove all guidance. CLAUDE.md is a symlink to
        # AGENTS.md — drop it too so the SDK finds no dangling link.
        (wt_dir / "AGENTS.md").unlink(missing_ok=True)
        (wt_dir / "CLAUDE.md").unlink(missing_ok=True)
    else:
        shutil.copy2(agents_file, wt_dir / "AGENTS.md")

    if skill_name and skill_file:
        skill_dir = wt_dir / ".agents" / "skills" / skill_name
        skill_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(skill_file, skill_dir / "SKILL.md")

    return wt_dir


def remove_worktree(wt_dir: Path) -> None:
    run(["git", "-C", str(REPO_ROOT), "worktree", "remove", "--force", str(wt_dir)])


def add_provider(
    config_lines: list[str],
    label: str,
    working_dir: Path,
    model: str,
    skill_name: str | None = None,
) -> None:
    config_lines.append("  - id: anthropic:claude-agent-sdk")
    config_lines.append(f"    label: {label}")
    config_lines.append("    config:")
    config_lines.append(f"      model: {model}")
    config_lines.append("      apiKeyRequired: false")
    config_lines.append("      setting_sources: ['project']")
    config_lines.append("      append_allowed_tools: ['Read', 'Grep', 'Glob']")
    config_lines.append(f"      working_dir: {working_dir}")
    if skill_name:
        config_lines.append(f"      skills: ['{skill_name}']")
    config_lines.append(OUTPUT_SCHEMA)
    config_lines.append("")


def main() -> int:
    check_prerequisites()

    model = os.environ.get("MODEL", "claude-sonnet-4-6")
    skill_name = os.environ.get("SKILL_NAME")
    skill_src = None
    if skill_name:
        skill_src = REPO_ROOT / ".agents" / "skills" / skill_name / "SKILL.md"
        if not skill_src.is_file():
            print(f"Error: {skill_src} not found", file=sys.stderr)
            return 1

    # Parse flags
    full_mode = "--full" in sys.argv
    promptfoo_args = [a for a in sys.argv[1:] if a != "--full"]

    if full_mode and skill_name:
        print(
            "Error: --full cannot be combined with SKILL_NAME — the baseline arm has"
            " no skill, so the 'skill-used' assertion would always fail on it.",
            file=sys.stderr,
        )
        return 1

    base_branch = resolve_base_branch()
    check_claude_md_symlink(base_branch)

    # Hash before building arms so edits made mid-run aren't recorded as tested.
    guidance_hash = compute_guidance_hash(AGENTS_SRC, CASES_DIR)

    # Temp dir for config and worktrees
    work_dir = Path(tempfile.mkdtemp())
    worktrees: list[Path] = []

    try:
        # Symlink node_modules for promptfoo SDK resolution
        sdk_modules = Path.home() / ".promptfoo-sdk" / "node_modules"
        (work_dir / "node_modules").symlink_to(sdk_modules)

        # Extract main-branch AGENTS.md
        main_agents = work_dir / "main-agents.md"
        if not git_show_file(base_branch, "AGENTS.md", main_agents):
            shutil.copy2(AGENTS_SRC, main_agents)
            print(f"Warning: AGENTS.md not found on {base_branch} — main arm uses working tree copy")

        main_skill = None
        if skill_name and skill_src:
            main_skill = work_dir / "main-skill.md"
            skill_git_path = f".agents/skills/{skill_name}/SKILL.md"
            if not git_show_file(base_branch, skill_git_path, main_skill):
                shutil.copy2(skill_src, main_skill)
                print(f"Warning: SKILL.md not found on {base_branch} — main arm uses working tree copy")

        # Detect changes to decide which arms to build
        agents_changed = run(["diff", "-q", str(main_agents), str(AGENTS_SRC)]).returncode != 0
        skill_changed = False
        if skill_name and main_skill and skill_src:
            skill_changed = run(["diff", "-q", str(main_skill), str(skill_src)]).returncode != 0
        need_working = agents_changed or skill_changed

        # Assemble arms. Prune first to self-heal stale worktree registrations
        # left behind by a previously interrupted run.
        print("Assembling arms (git worktrees) ...")
        run(["git", "-C", str(REPO_ROOT), "worktree", "prune"])

        arm_main = create_worktree(
            work_dir, "main", base_branch, main_agents, worktrees, skill_name, main_skill
        )

        arm_working = None
        if need_working:
            arm_working = create_worktree(
                work_dir, "working", base_branch, AGENTS_SRC, worktrees, skill_name, skill_src
            )
        else:
            print("  AGENTS.md unchanged — skipping working arm")

        arm_baseline = None
        if full_mode:
            arm_baseline = create_worktree(work_dir, "baseline", base_branch, None, worktrees)

        # Generate config
        config_lines: list[str] = []
        config_lines.append("prompts:")
        config_lines.append("  - '{{request}}'")
        config_lines.append("")
        config_lines.append("providers:")

        arm_count = 0
        skill_for_provider = skill_name if skill_name else None

        add_provider(config_lines, "main", arm_main, model, skill_for_provider)
        arm_count += 1

        if arm_working:
            add_provider(config_lines, "working", arm_working, model, skill_for_provider)
            arm_count += 1

        if full_mode and arm_baseline:
            add_provider(config_lines, "baseline", arm_baseline, model)
            arm_count += 1

        # Default test config
        config_lines.append("defaultTest:")
        config_lines.append("  options:")
        config_lines.append("    disableVarExpansion: true")
        if skill_name:
            config_lines.append("  assert:")
            config_lines.append("    - type: skill-used")
            config_lines.append(f"      value: {skill_name}")
        config_lines.append("")

        # Cases
        config_lines.append(f"tests: file://{SCRIPT_DIR}/cases/*.yaml")

        config_path = work_dir / "promptfooconfig.yaml"
        config_path.write_text("\n".join(config_lines) + "\n")

        # Report
        if skill_name:
            print(f"Mode: {arm_count} arms, skill '{skill_name}', model: {model}")
        else:
            print(f"Mode: {arm_count} arms, AGENTS.md only, model: {model}")

        print()
        print(f"Changes detected (vs {base_branch}):")
        print(f"  AGENTS.md — {'modified' if agents_changed else 'unchanged'}")
        if skill_name:
            print(f"  SKILL.md  — {'modified' if skill_changed else 'unchanged'} ({skill_name})")
        print()

        # Run promptfoo
        result = subprocess.run(
            ["npx", f"promptfoo@{PROMPTFOO_VERSION}", "eval", "-c", str(config_path), *promptfoo_args],
            check=False,
        )

        # 0 = all passed, 100 = some assertions failed — both mean the eval
        # completed and the results were seen, which is what the hash proves.
        if result.returncode in (0, 100):
            write_recorded_hash(guidance_hash, HASH_FILE)
            print()
            print(f"Recorded eval run in {HASH_FILE.relative_to(REPO_ROOT)} — commit it with your change.")

        print()
        print(f"View results: npx promptfoo@{PROMPTFOO_VERSION} view")
        return result.returncode

    finally:
        for wt in worktrees:
            remove_worktree(wt)
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
