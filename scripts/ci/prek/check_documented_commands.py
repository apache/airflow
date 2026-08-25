#!/usr/bin/env python
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
"""Check that every breeze/prek command the docs mention actually exists.

Documentation can drift from reality in two directions: someone documents a
command that never existed (apache/airflow#69861 shipped
``breeze selective-checks`` — the registered command is
``breeze ci selective-check``), or a real command or option gets renamed
while the docs stay put (apache/airflow#43979 renamed
``breeze testing integration-tests`` and the same commit left a doc line
pointing at the old name). The sync pipeline (generate-agent-skills, #68204)
only copies the generated Commands section of AGENTS.md from
contributing-docs; this hook is the sole validator, checking the rendered
docs — generated and hand-written alike — against what the CLIs actually
register. It scans AGENTS.md and the whole ``contributing-docs/`` tree, and
also triggers on changes under ``dev/breeze/src/airflow_breeze/commands/``
and the prek configs, so a rename re-validates the docs without any doc
change.

Two families are validated (precision over recall — a checker that cries wolf
gets SKIPped):

- ``breeze …`` — against Breeze's click tree, introspected at runtime via
  ``breeze_command_walker.py``: subcommands and ``--long`` options both,
  except the forwarded tail of a ``passthrough`` command.
- ``prek run <hook-id>`` — against the hook ids in every
  ``.pre-commit-config.yaml`` (root and nested).

A ``<placeholder>`` word stops validation rather than guessing.
"""

from __future__ import annotations

import difflib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_ROOT_PATH, check_uv_version, console

BREEZE_PROJECT_PATH = AIRFLOW_ROOT_PATH / "dev" / "breeze"
BREEZE_WALKER_PATH = Path(__file__).parent.resolve() / "breeze_command_walker.py"

# Words that end a command path: options, placeholders, shell syntax, paths.
_COMMAND_WORD_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")

# ``double`` = RST literal, `single` = Markdown code. The lookahead drops a
# single-backtick span closed by ``_`` — an RST hyperlink (`text <url>`_), not code.
_INLINE_CODE_RE = re.compile(r"``([^`]+)``|`([^`\n]+)`(?!_)")
_FENCE_RE = re.compile(r"^(```|~~~)")
_HEREDOC_START_RE = re.compile(r"<<-?\s*'?\"?(\w+)")

# RST literal blocks are indented, not fenced: a ``.. code-block::`` or a
# paragraph ending ``::`` starts one, until the indentation drops back. Other
# ``.. directive::`` forms (``.. note::`` …) aren't literal blocks.
_RST_CODE_DIRECTIVE_RE = re.compile(r"^\.\.\s+(?:code-block|code|sourcecode)::")
_RST_DIRECTIVE_RE = re.compile(r"^\.\.\s+\S+::")


class BreezeIntrospectionError(RuntimeError):
    """Breeze's command tree could not be introspected (broken env, import error…)."""


@dataclass(frozen=True)
class BreezeCommand:
    """One node of Breeze's click command tree."""

    group: bool
    passthrough: bool
    opts: frozenset[str]


@dataclass(frozen=True)
class DocumentedCommand:
    """A command candidate extracted from documentation text."""

    family: str  # "breeze" | "prek"
    text: str  # the command path, e.g. "breeze ci selective-check" (for messages)
    raw: str  # the original snippet — validated in full (path + options)


@dataclass(frozen=True)
class CommandViolation:
    command: DocumentedCommand
    reason: str


def _cut_command_words(snippet: str) -> list[str]:
    """Keep leading command-path words, stopping at options/placeholders/args."""
    words = []
    for word in snippet.split():
        if not _COMMAND_WORD_RE.match(word) or "<" in word:
            break
        words.append(word)
    return words


def _extract_from_snippet(snippet: str) -> DocumentedCommand | None:
    snippet = snippet.strip()
    if snippet.startswith("breeze "):
        words = _cut_command_words(snippet)
        if len(words) >= 2:
            return DocumentedCommand(family="breeze", text=" ".join(words), raw=snippet)
    elif snippet.startswith("prek run "):
        parts = snippet.split()
        hook_id = parts[2] if len(parts) > 2 else ""
        # No hook id (bare `prek run --from-ref …`) or a placeholder id
        # (`mypy-<project>`) cannot be validated — skip.
        if hook_id and not hook_id.startswith("-") and "<" not in hook_id:
            return DocumentedCommand(family="prek", text=hook_id, raw=snippet)
    return None


def _iter_inline_code(line: str) -> list[str]:
    """Yield the code text of every inline span on a line (double- or single-backtick)."""
    return [double or single for double, single in _INLINE_CODE_RE.findall(line)]


def extract_documented_commands(text: str) -> list[DocumentedCommand]:
    """Extract breeze/prek command candidates from Markdown/RST text.

    Scans inline code spans, fenced (```) blocks, and RST indented literal
    blocks. Heredoc bodies inside fences are skipped — their content is data,
    not commands.
    """
    candidates: list[DocumentedCommand] = []
    in_fence = False
    heredoc_terminator: str | None = None
    literal_indent: int | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if _FENCE_RE.match(stripped):
            in_fence = not in_fence
            heredoc_terminator = None
            literal_indent = None
            continue
        if in_fence:
            if heredoc_terminator:
                if stripped == heredoc_terminator:
                    heredoc_terminator = None
                continue
            heredoc_match = _HEREDOC_START_RE.search(line)
            if heredoc_match:
                heredoc_terminator = heredoc_match.group(1)
            candidate = _extract_from_snippet(line)
            if candidate:
                candidates.append(candidate)
            continue
        if literal_indent is not None:
            if stripped == "":
                continue
            if len(line) - len(line.lstrip()) > literal_indent:
                candidate = _extract_from_snippet(line)
                if candidate:
                    candidates.append(candidate)
                continue
            literal_indent = None  # dedented out of the literal block; process normally
        for span in _iter_inline_code(line):
            candidate = _extract_from_snippet(span)
            if candidate:
                candidates.append(candidate)
        if _RST_CODE_DIRECTIVE_RE.match(stripped):
            literal_indent = len(line) - len(line.lstrip())
        elif not _RST_DIRECTIVE_RE.match(stripped) and stripped.endswith("::"):
            literal_indent = len(line) - len(line.lstrip())
    return candidates


def build_breeze_registry() -> dict[str, BreezeCommand]:
    """Introspect Breeze's click command tree, keyed by command path."""
    check_uv_version()
    result = subprocess.run(
        ["uv", "run", "--project", str(BREEZE_PROJECT_PATH), "python", str(BREEZE_WALKER_PATH)],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "SKIP_BREEZE_SELF_UPGRADE_CHECK": "true"},
    )
    for line in result.stdout.splitlines():
        if line.startswith("REGISTRY_JSON:"):
            raw_tree = json.loads(line[len("REGISTRY_JSON:") :])
            return {
                path: BreezeCommand(
                    group=node["group"],
                    passthrough=node["passthrough"],
                    opts=frozenset(node["opts"]),
                )
                for path, node in raw_tree.items()
            }
    raise BreezeIntrospectionError(
        f"Could not introspect the breeze command tree (exit {result.returncode}):\n"
        f"{result.stderr.strip()[:2000]}"
    )


def build_prek_hook_registry() -> frozenset[str]:
    """Collect hook ids from the root and nested .pre-commit-config.yaml files."""
    hook_ids: set[str] = set()
    for config_path in AIRFLOW_ROOT_PATH.rglob(".pre-commit-config.yaml"):
        if any(part in (".venv", "node_modules", ".build") for part in config_path.parts):
            continue
        config = yaml.safe_load(config_path.read_text()) or {}
        for repo in config.get("repos", []):
            for hook in repo.get("hooks", []) or []:
                if isinstance(hook, dict) and hook.get("id"):
                    hook_ids.add(str(hook["id"]))
    return frozenset(hook_ids)


def _suggest(word: str, options: frozenset[str] | set[str]) -> str:
    close = difflib.get_close_matches(word, sorted(options), n=2, cutoff=0.6)
    return f" — did you mean {' or '.join(f'`{c}`' for c in close)}?" if close else ""


def _validate_breeze(raw: str, registry: dict[str, BreezeCommand]) -> str | None:
    """Walk a breeze snippet against the tree; return a violation reason or None."""
    words = raw.split()
    node = words[0]
    if node not in registry:
        return "not a registered breeze command (breeze --help)"
    index = 1
    while index < len(words):
        word = words[index]
        if word.startswith("<"):
            return None  # documented placeholder — cannot descend further
        if word.startswith("-") or not _COMMAND_WORD_RE.match(word):
            break  # options begin, or a positional argument / path
        if not registry[node].group:
            break  # leaf reached; trailing words are its arguments
        nxt = f"{node} {word}"
        if nxt not in registry:
            subcommands = {p.rsplit(" ", 1)[1] for p in registry if p.startswith(f"{node} ")}
            return f"`{word}` is not a subcommand of `{node}`{_suggest(word, subcommands)}"
        node, index = nxt, index + 1
    command = registry[node]
    if command.passthrough:
        return None  # tail is forwarded to another program; options are not ours to check
    for word in words[index:]:
        if word.startswith("--") and word != "--help":  # click adds --help to every command
            option = word.split("=", 1)[0]
            if option not in command.opts:
                return f"`{option}` is not an option of `{node}`{_suggest(option, command.opts)}"
    return None


def find_violations(
    candidates: list[DocumentedCommand],
    breeze_registry: dict[str, BreezeCommand],
    prek_registry: frozenset[str],
) -> list[CommandViolation]:
    """Return candidates that match no registered command or option."""
    violations = []
    for candidate in candidates:
        if candidate.family == "breeze":
            reason = _validate_breeze(candidate.raw, breeze_registry)
            if reason:
                violations.append(CommandViolation(candidate, reason))
        elif candidate.family == "prek" and candidate.text not in prek_registry:
            reason = f"not a hook id in any .pre-commit-config.yaml{_suggest(candidate.text, prek_registry)}"
            violations.append(CommandViolation(candidate, reason))
    return violations


def _doc_paths() -> list[Path]:
    return [
        AIRFLOW_ROOT_PATH / "AGENTS.md",
        *sorted((AIRFLOW_ROOT_PATH / "contributing-docs").rglob("*.rst")),
    ]


def main() -> int:
    per_file = [(path, extract_documented_commands(path.read_text())) for path in _doc_paths()]
    total = sum(len(candidates) for _, candidates in per_file)
    if not total:
        console.print("[yellow]No breeze/prek commands found in the docs — nothing to check.[/yellow]")
        return 0
    try:
        breeze_registry = build_breeze_registry()
    except BreezeIntrospectionError as error:
        # Don't block a doc commit on a broken breeze env; a real missing
        # command still fails once introspection works again.
        console.print(
            f"[yellow]Skipping documented-command check — breeze introspection failed:[/yellow]\n{error}"
        )
        return 0
    prek_registry = build_prek_hook_registry()
    violations = [
        (path, violation)
        for path, candidates in per_file
        for violation in find_violations(candidates, breeze_registry, prek_registry)
    ]
    if not violations:
        console.print(f"[green]All {total} documented commands across {len(per_file)} files exist.[/green]")
        return 0
    console.print(f"[red]The docs mention {len(violations)} command(s) that do not exist:[/red]")
    for path, violation in violations:
        console.print(
            f"  [red]✗[/red] `{violation.command.raw}` — {violation.reason}\n"
            f"      in {path.relative_to(AIRFLOW_ROOT_PATH)}"
        )
    console.print(
        "\nFix the command in the doc file shown. For the generated Commands"
        " section of AGENTS.md, fix the AGENT-SKILL source block in contributing-docs"
        " instead (see generate_agent_skills.py). If a command was renamed, update"
        " the docs to the new name."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
