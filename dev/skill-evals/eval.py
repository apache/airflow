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
    prek run run-skill-eval --hook-stage manual --all-files

Env knobs: MODEL, SKILL_NAME, EVAL_REPEAT, EVAL_FULL (baseline arm).
Promptfoo flags like --filter* are argv-only — wire them as fixed entry
args on a hook variant when needed.

Authentication: Claude Code session (claude /login) or ANTHROPIC_API_KEY.
"""

from __future__ import annotations

import json
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

# Tool state lives under .build (repo-scoped, established cleanup culture);
# per-run reports go to files/ per the AGENTS.md output convention.
PROMPTFOO_STATE_DIR = REPO_ROOT / ".build" / "promptfoo"
RESULTS_FILE = REPO_ROOT / "files" / "skill-evals" / "results.json"

# Shared with the check-eval-hash prek hook so recorded and verified hashes can't drift.
sys.path.insert(0, str(REPO_ROOT / "scripts" / "ci" / "prek"))
from check_eval_hash import compute_guidance_hash, write_recorded_hash  # noqa: E402

OUTPUT_FORMAT = {
    "type": "json_schema",
    "schema": {
        "type": "object",
        "required": ["should_create", "rationale"],
        "additionalProperties": False,
        "properties": {
            "should_create": {"type": "boolean"},
            "type": {
                "type": "string",
                "enum": ["bugfix", "feature", "improvement", "doc", "misc", "significant"],
            },
            "rationale": {"type": "string"},
        },
    },
}


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=False, **kwargs)


def find_sdk_modules() -> Path:
    """Locate the node_modules dir (in the prek env) that contains the Claude Agent SDK.

    promptfoo resolves the SDK from the eval config's directory, not from its
    own install tree — the caller symlinks this dir next to the config.
    """
    promptfoo_bin = shutil.which("promptfoo")
    if promptfoo_bin:
        # PROMPTFOO_DISABLE_UPDATE avoids an outdated version banner on stdout when a newer
        # promptfoo is published upstream, which would otherwise break this exact match.
        version = run(
            ["promptfoo", "--version"], env={**os.environ, "PROMPTFOO_DISABLE_UPDATE": "1"}
        ).stdout.strip()
        if version == PROMPTFOO_VERSION:
            pf_pkg = Path(promptfoo_bin).resolve()
            while pf_pkg.name != "promptfoo" or not (pf_pkg / "package.json").is_file():
                if pf_pkg.parent == pf_pkg:
                    break
                pf_pkg = pf_pkg.parent
            for candidate in (pf_pkg / "node_modules", pf_pkg.parent):
                if (candidate / "@anthropic-ai" / "claude-agent-sdk").is_dir():
                    return candidate
    print(
        "Error: promptfoo with the Claude Agent SDK not found. Run the eval via:\n"
        "  prek run run-skill-eval --hook-stage manual --all-files",
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


def build_provider(label: str, working_dir: Path, model: str, skill_name: str | None = None) -> dict:
    config: dict = {
        "model": model,
        "apiKeyRequired": False,
        "setting_sources": ["project"],
        "append_allowed_tools": ["Read", "Grep", "Glob"],
        "working_dir": str(working_dir),
        "output_format": OUTPUT_FORMAT,
    }
    if skill_name:
        config["skills"] = [skill_name]
    return {"id": "anthropic:claude-agent-sdk", "label": label, "config": config}


def count_provider_errors(results_file: Path) -> int:
    """Count results whose provider call errored (as opposed to failing an assertion)."""
    try:
        results = json.loads(results_file.read_text())["results"]["results"]
    except (OSError, KeyError, ValueError):
        return 0
    return sum(1 for r in results if (r.get("response") or {}).get("error"))


def main() -> int:
    sdk_modules = find_sdk_modules()

    model = os.environ.get("MODEL", "claude-sonnet-4-6")
    skill_name = os.environ.get("SKILL_NAME")
    skill_src = None
    if skill_name:
        skill_src = REPO_ROOT / ".agents" / "skills" / skill_name / "SKILL.md"
        if not skill_src.is_file():
            print(f"Error: {skill_src} not found", file=sys.stderr)
            return 1

    # Parse flags — argv (hook entry args) plus single-value env knobs,
    # since `prek run` can't forward arguments. No filter knob on purpose:
    # a lingering export must not be able to change what the proof claims.
    full_mode = "--full" in sys.argv or os.environ.get("EVAL_FULL", "").lower() in ("1", "true")
    promptfoo_args = [a for a in sys.argv[1:] if a != "--full"]
    repeat = os.environ.get("EVAL_REPEAT")
    if repeat:
        if not repeat.isdigit() or int(repeat) < 1:
            print(f"Error: EVAL_REPEAT must be a positive integer, got {repeat!r}", file=sys.stderr)
            return 1
        promptfoo_args += ["--repeat", repeat]

    if full_mode and skill_name:
        print(
            "Error: EVAL_FULL/--full cannot be combined with SKILL_NAME — the baseline"
            " arm has no skill, so the 'skill-used' assertion would always fail on it.",
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
        # promptfoo resolves the Claude Agent SDK from the config directory
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

        # Generate config (JSON — valid promptfoo config, keeps the script stdlib-only)
        providers = [build_provider("main", arm_main, model, skill_name)]
        if arm_working:
            providers.append(build_provider("working", arm_working, model, skill_name))
        if full_mode and arm_baseline:
            providers.append(build_provider("baseline", arm_baseline, model))

        default_test: dict = {"options": {"disableVarExpansion": True}}
        if skill_name:
            default_test["assert"] = [{"type": "skill-used", "value": skill_name}]

        config = {
            "prompts": ["{{request}}"],
            "providers": providers,
            "defaultTest": default_test,
            "tests": f"file://{SCRIPT_DIR}/cases/*.yaml",
        }
        config_path = work_dir / "promptfooconfig.json"
        config_path.write_text(json.dumps(config, indent=2))

        # Report
        if skill_name:
            print(f"Mode: {len(providers)} arms, skill '{skill_name}', model: {model}")
        else:
            print(f"Mode: {len(providers)} arms, AGENTS.md only, model: {model}")

        print()
        print(f"Changes detected (vs {base_branch}):")
        print(f"  AGENTS.md — {'modified' if agents_changed else 'unchanged'}")
        if skill_name:
            print(f"  SKILL.md  — {'modified' if skill_changed else 'unchanged'} ({skill_name})")
        print()

        # Run promptfoo — state under .build, per-run report under files/
        PROMPTFOO_STATE_DIR.mkdir(parents=True, exist_ok=True)
        RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            ["promptfoo", "eval", "-c", str(config_path), "--output", str(RESULTS_FILE), *promptfoo_args],
            check=False,
            env={**os.environ, "PROMPTFOO_CONFIG_DIR": str(PROMPTFOO_STATE_DIR)},
        )

        # 0 = all passed, 100 = some assertions failed — both mean the eval
        # completed and the results were seen, which is what the hash proves.
        # Don't record partial runs (--filter* covers a subset of cases) or
        # runs with provider errors (nothing was actually evaluated).
        partial_run = any(arg.startswith("--filter") for arg in promptfoo_args)
        provider_errors = count_provider_errors(RESULTS_FILE)
        if result.returncode in (0, 100) and not partial_run and not provider_errors:
            write_recorded_hash(guidance_hash, HASH_FILE)
            print()
            print(f"Recorded eval run in {HASH_FILE.relative_to(REPO_ROOT)} — commit it with your change.")
        elif partial_run:
            print()
            print("Partial run (--filter*) — hash not recorded; run the full case set to update it.")
        elif provider_errors:
            print()
            print(f"{provider_errors} provider error(s) — hash not recorded; fix the setup and rerun.")

        print()
        print(f"Results report: {RESULTS_FILE.relative_to(REPO_ROOT)}")
        print(
            f"View results: PROMPTFOO_CONFIG_DIR={PROMPTFOO_STATE_DIR.relative_to(REPO_ROOT)}"
            f" npx promptfoo@{PROMPTFOO_VERSION} view"
        )
        return result.returncode

    finally:
        for wt in worktrees:
            remove_worktree(wt)
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
