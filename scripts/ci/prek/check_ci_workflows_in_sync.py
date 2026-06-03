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
"""
Verify ci-arm.yml and ci-amd.yml stay in sync.

The two workflows are physical copies of each other (GitHub Actions has no
cross-file YAML include) and may differ ONLY in the small set of intentional
divergences listed below. This check normalizes both files against that
allowlist, then asserts the rest matches byte-for-byte.

Documented divergences:

1. Header intro comment (different prose explaining what each file is for)
2. Workflow ``name:`` — ``Tests (ARM)`` vs ``Tests (AMD)``
3. Triggers — ARM = its own schedule + push (to release-prep /
   providers branches) + workflow_dispatch; AMD = its own schedule
   (offset from ARM's cron) + the same push branches + pull_request +
   workflow_dispatch. Both wrappers run on post-merge pushes to
   stable / providers branches; only AMD runs on PRs (per-PR ARM
   coverage stays cron-driven via the canary).
4. ``concurrency.group`` prefix — ``ci-arm-`` vs ``ci-amd-``
5. ``build-info`` outputs ``platform`` and ``runner-type`` — hardcoded per
   architecture (and the surrounding comment naming the "ARM/AMD copy")
6. ``print-platform`` job — ``name:`` and the architecture echoed to
   GITHUB_STEP_SUMMARY
7. ``notify-slack`` Slack-state artifact name — ``slack-state-tests-…-arm``
   vs ``slack-state-tests-…-amd``, so the de-dup tracker in
   ``slack_notification_state.py`` keeps independent state for each
   platform on the same branch

Anything else differing between the two files is a drift bug. To
intentionally introduce a new divergence, update the rules in this script
in the same PR.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_ci_workflows_in_sync.py

Exits 0 if the workflows are in sync, 1 (with a diff) otherwise.

Local UX
--------

When run interactively (TTY attached, ``CI`` env var unset) the script
also tries to figure out which side changed since the branch diverged
from ``main`` and prints concrete next steps — either mirror the change
to the other file, or add it to the per-line / per-block allowlist below.
"""

from __future__ import annotations

import difflib
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
ARM = REPO_ROOT / ".github" / "workflows" / "ci-arm.yml"
AMD = REPO_ROOT / ".github" / "workflows" / "ci-amd.yml"
THIS_SCRIPT = Path(__file__).relative_to(REPO_ROOT)

# Per-line regex normalizations applied to BOTH files. Each rule replaces
# the architecture-specific token with a placeholder so the two normalized
# files become identical. If you add a per-line difference between ARM and
# AMD, append a rule here.
LINE_RULES: list[tuple[str, str]] = [
    (r"^name: Tests \((?:ARM|AMD)\)$", "name: Tests (PLACEHOLDER)"),
    (r"^  group: ci-(?:arm|amd)-", "  group: ci-PLACEHOLDER-"),
    (r'^      platform: "linux/(?:arm64|amd64)"$', '      platform: "linux/PLACEHOLDER"'),
    (r"^      runner-type: '\[\"ubuntu-22\.04(?:-arm)?\"\]'$", "      runner-type: 'PLACEHOLDER'"),
    (
        r"^      # (?:ARM|AMD) copy\)\. The matching (?:AMD|ARM) copy lives in ci-(?:amd|arm)\.yml\.$",
        "      # PLACEHOLDER copy). The matching PLACEHOLDER copy lives in ci-PLACEHOLDER.yml.",
    ),
    (r'^    name: "Platform: (?:ARM|AMD)"$', '    name: "Platform: PLACEHOLDER"'),
    (
        r"^        run: \"echo '## Architecture: (?:ARM|AMD)' >> \$GITHUB_STEP_SUMMARY\"$",
        "        run: \"echo '## Architecture: PLACEHOLDER' >> $GITHUB_STEP_SUMMARY\"",
    ),
    # Slack-state artifact name in the notify-slack job — suffixed `-amd` /
    # `-arm` so each platform tracks its own de-dup state on the same branch.
    (
        r'^(?P<indent>\s+)(?P<key>ARTIFACT_NAME|name): "slack-state-tests-\$\{\{ github\.ref_name \}\}-(?:amd|arm)"$',
        r'\g<indent>\g<key>: "slack-state-tests-${{ github.ref_name }}-PLACEHOLDER"',
    ),
]

# Whole sections that legitimately exist in only one file. Each entry is the
# verbatim block (including trailing newline) that must appear in the named
# file and must NOT appear in the other. The blocks are stripped before
# diffing so they don't show up as drift.
ARM_ONLY_BLOCK = """  schedule:
    - cron: '28 1,3,7,9,13,15,19,21 * * *'
"""

AMD_ONLY_BLOCK = """  schedule:
    # Mirror of the previous AMD canary cron from before the AMD/ARM split (PR #66348),
    # offset by 30 min from ARM's `:28` slot in `ci-arm.yml` so the two scheduled
    # canaries don't compete for runners at exactly the same minute.
    - cron: '58 1,7,13,19 * * *'
  pull_request:
    branches:
      - main
      - v[0-9]+-[0-9]+-test
      - v[0-9]+-[0-9]+-stable
      - providers-[a-z]+-?[a-z]*/v[0-9]+-[0-9]+
    types: [opened, reopened, synchronize, ready_for_review]
"""


# The header comment block (between the `---` document marker and the
# `name:` line) is intentionally different between files — each describes
# what its own file is for. Collapse to a placeholder before diffing.
HEADER_PATTERN = re.compile(r"(?<=\n---\n)(.*?)(?=^name: )", re.MULTILINE | re.DOTALL)
HEADER_PLACEHOLDER = "# PLACEHOLDER: per-file intro comment\n"


# --- ANSI color helpers -------------------------------------------------------
# Local UX: colorize the report when stdout is a TTY and we're not in CI.

_INTERACTIVE = os.environ.get("FORCE_INTERACTIVE") == "1" or (
    sys.stdout.isatty() and not os.environ.get("CI") and not os.environ.get("PREK_HEADLESS")
)


def _ansi(code: str, text: str) -> str:
    if not _INTERACTIVE:
        return text
    return f"\033[{code}m{text}\033[0m"


def red(t: str) -> str:
    return _ansi("1;31", t)


def green(t: str) -> str:
    return _ansi("1;32", t)


def yellow(t: str) -> str:
    return _ansi("1;33", t)


def cyan(t: str) -> str:
    return _ansi("1;36", t)


def bold(t: str) -> str:
    return _ansi("1", t)


# --- normalization ------------------------------------------------------------


def normalize(content: str, *, side: str) -> str:
    """Strip side-specific blocks and apply line rules so both files compare equal."""
    if side == "arm":
        if ARM_ONLY_BLOCK not in content:
            raise SystemExit(f"ci-arm.yml is missing its expected ARM-only trigger block:\n{ARM_ONLY_BLOCK}")
        if AMD_ONLY_BLOCK in content:
            raise SystemExit("ci-arm.yml contains the AMD-only trigger block — should not.")
        content = content.replace(ARM_ONLY_BLOCK, "")
    elif side == "amd":
        if AMD_ONLY_BLOCK not in content:
            raise SystemExit(f"ci-amd.yml is missing its expected AMD-only trigger block:\n{AMD_ONLY_BLOCK}")
        if ARM_ONLY_BLOCK in content:
            raise SystemExit("ci-amd.yml contains the ARM-only trigger block — should not.")
        content = content.replace(AMD_ONLY_BLOCK, "")
    else:
        raise ValueError(f"Unknown side: {side}")

    for pattern, replacement in LINE_RULES:
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    content = HEADER_PATTERN.sub(HEADER_PLACEHOLDER, content)
    return content


# --- main-branch comparison (local UX) ----------------------------------------


def _git(args: list[str]) -> str | None:
    """Run git from the repo root; return stdout text on success, None on failure."""
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), *args],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout


def _resolve_baseline_ref() -> str | None:
    """Pick a reasonable baseline ref to diff against — the closest of upstream/main, origin/main, or main."""
    for candidate in ("upstream/main", "origin/main", "main"):
        if _git(["rev-parse", "--verify", "--quiet", candidate]) is not None:
            return candidate
    return None


def _diff_since_baseline(file_path: Path, baseline: str) -> str | None:
    """Return the unified diff of ``file_path`` from baseline to the working tree.

    Uses ``git diff <baseline> -- <file>`` (NOT the three-dot form) so the
    comparison covers committed AND uncommitted edits. The check needs to
    catch a developer who edits one file and forgets to mirror it before
    even running ``git add``.
    """
    rel = file_path.relative_to(REPO_ROOT).as_posix()
    output = _git(["diff", "--no-color", baseline, "--", rel])
    if output is None:
        return None
    return output.strip() or None


def _is_new_file_diff(diff: str | None) -> bool:
    """True if the diff describes the file as added (no baseline content to compare)."""
    return (
        bool(diff) and "new file mode" in diff.splitlines()[1]
        if diff and len(diff.splitlines()) > 1
        else False
    )


def _trim_diff(diff: str, max_lines: int = 60) -> str:
    """Cap diff to max_lines so the report stays readable; mention how many lines were dropped."""
    lines = diff.splitlines()
    if len(lines) <= max_lines:
        return diff
    dropped = len(lines) - max_lines
    return "\n".join(
        lines[:max_lines] + [f"... ({dropped} more lines suppressed; run `git diff` to see full)"]
    )


def _suggest_actions(arm_diff: str | None, amd_diff: str | None) -> None:
    """Print concrete suggestions about which file to edit, based on which side changed."""
    print()
    print(bold("Suggested next steps:"))
    print()
    arm_new = _is_new_file_diff(arm_diff)
    amd_new = _is_new_file_diff(amd_diff)
    if arm_new and amd_new:
        print(
            f"  • Both {cyan('ci-arm.yml')} and {cyan('ci-amd.yml')} were added in this branch — "
            f"there is no baseline to attribute the drift to. Compare the two files directly "
            f"({cyan('diff .github/workflows/ci-arm.yml .github/workflows/ci-amd.yml')}) and "
            f"either pick one as the source of truth or extend the allowlist in "
            f"{cyan(str(THIS_SCRIPT))}."
        )
        return
    if arm_diff and not amd_diff:
        print(
            f"  • Only {cyan('ci-arm.yml')} has changed since the baseline. To bring AMD in line, "
            f"mirror the change into {cyan('ci-amd.yml')}:"
        )
        print()
        print(_indent(_trim_diff(arm_diff)))
        print()
        print(
            f"  • If the change is intentionally ARM-only, add the new lines to "
            f"{cyan('LINE_RULES')} (or {cyan('ARM_ONLY_BLOCK')}) in {cyan(str(THIS_SCRIPT))}."
        )
    elif amd_diff and not arm_diff:
        print(
            f"  • Only {cyan('ci-amd.yml')} has changed since the baseline. To bring ARM in line, "
            f"mirror the change into {cyan('ci-arm.yml')}:"
        )
        print()
        print(_indent(_trim_diff(amd_diff)))
        print()
        print(
            f"  • If the change is intentionally AMD-only, add the new lines to "
            f"{cyan('LINE_RULES')} (or {cyan('AMD_ONLY_BLOCK')}) in {cyan(str(THIS_SCRIPT))}."
        )
    elif arm_diff and amd_diff:
        print(
            "  • Both files have changes since the baseline. Compare the two diffs below "
            "and decide which side is the source of truth, then mirror manually."
        )
        print()
        print(yellow("ci-arm.yml diff vs baseline:"))
        print(_indent(_trim_diff(arm_diff)))
        print()
        print(yellow("ci-amd.yml diff vs baseline:"))
        print(_indent(_trim_diff(amd_diff)))
        print()
        print(f"  • If the divergence is intentional, document it in {cyan(str(THIS_SCRIPT))}.")
    else:
        # Neither file changed against the baseline, but the normalized diff is non-empty.
        # That means the drift was already present before the branch — likely a bug in this
        # script's allowlist or a pre-existing inconsistency that nobody caught.
        print(
            f"  • Neither file changed since the baseline, yet the normalized diff is non-empty. "
            f"This usually means {cyan(str(THIS_SCRIPT))}'s allowlist is missing a rule for a "
            f"pre-existing divergence. Inspect the diff above and update LINE_RULES / "
            f"ARM_ONLY_BLOCK / AMD_ONLY_BLOCK accordingly."
        )


def _indent(text: str, prefix: str = "      ") -> str:
    return "\n".join(prefix + line for line in text.splitlines())


# --- entry point --------------------------------------------------------------


def main() -> int:
    if not ARM.exists() or not AMD.exists():
        print(red(f"ERROR: expected both {ARM.name} and {AMD.name} to exist under .github/workflows/."))
        return 1

    arm_normalized = normalize(ARM.read_text(), side="arm")
    amd_normalized = normalize(AMD.read_text(), side="amd")

    if arm_normalized == amd_normalized:
        print(green(f"OK: {ARM.name} and {AMD.name} are in sync (apart from documented divergences)."))
        return 0

    print(red(f"ERROR: {ARM.name} and {AMD.name} have diverged outside the allowed set."))
    print()
    print(bold("Normalized diff:"))
    print()
    diff = difflib.unified_diff(
        arm_normalized.splitlines(),
        amd_normalized.splitlines(),
        fromfile=f"{ARM.name} (normalized)",
        tofile=f"{AMD.name} (normalized)",
        lineterm="",
    )
    for line in diff:
        if line.startswith("+") and not line.startswith("+++"):
            print(green(line))
        elif line.startswith("-") and not line.startswith("---"):
            print(red(line))
        elif line.startswith("@@"):
            print(cyan(line))
        else:
            print(line)

    if _INTERACTIVE:
        baseline = _resolve_baseline_ref()
        if baseline is None:
            print()
            print(
                yellow(
                    "Could not resolve a baseline ref (tried upstream/main, origin/main, main). "
                    "Skipping the per-side change attribution; fix manually using the diff above."
                )
            )
            return 1
        arm_diff = _diff_since_baseline(ARM, baseline)
        amd_diff = _diff_since_baseline(AMD, baseline)
        print()
        print(bold(f"Comparing each file against {cyan(baseline)} to identify the changed side…"))
        _suggest_actions(arm_diff, amd_diff)
    else:
        print()
        print(
            yellow(
                "Run this script locally outside CI to see which side changed and get a "
                "concrete suggestion (mirror the change, or add it to the allowlist in this script)."
            )
        )

    return 1


if __name__ == "__main__":
    sys.exit(main())
