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
from __future__ import annotations

import json
import shutil
import sys
from collections.abc import Callable

from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.github import PRAssessment, Violation
from airflow_breeze.utils.run_utils import run_command

MAX_PR_BODY_CHARS = 3000
MAX_CHECK_RUNS = 30

_SYSTEM_PROMPT_PREFIX = """\
You are a pull request quality reviewer for the Apache Airflow open-source project.
Your job is to assess whether a PR meets minimum quality criteria for maintainer review.

NOTE: CI check failures (pre-commit, linting, mypy, tests) are detected automatically and handled
separately — do NOT evaluate them. Focus only on the criteria below.
"""

_SYSTEM_PROMPT_SUFFIX = """
Respond with JSON only (no markdown fences). Use this schema:
{
  "should_flag": true/false,
  "violations": [
    {"category": "...", "explanation": "...", "severity": "error|warning"}
  ],
  "summary": "One-sentence summary of the assessment."
}

Only set should_flag=true if there is at least one violation with severity "error".
Be strict but fair — the goal is to catch clearly low-quality PRs, not to nitpick.
"""

# Paths relative to the Airflow root
_PR_GUIDELINES_PATH = "contributing-docs/05_pull_requests.rst"
_CODE_REVIEW_PATH = ".github/instructions/code-review.instructions.md"


def _read_file_section(root_path, relative_path: str, start_marker: str, end_marker: str) -> str:
    """Read a section of a file between start and end markers (exclusive)."""
    filepath = root_path / relative_path
    if not filepath.is_file():
        return ""
    lines = filepath.read_text().splitlines()
    capturing = False
    captured: list[str] = []
    for line in lines:
        if not capturing and start_marker in line:
            capturing = True
            continue
        if capturing and end_marker in line:
            break
        if capturing:
            captured.append(line)
    return "\n".join(captured).strip()


def _load_system_prompt() -> str:
    """Build the LLM system prompt from project documentation files."""
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

    # Read PR quality criteria from contributing docs (use the RST label as unique anchor)
    pr_criteria = _read_file_section(
        AIRFLOW_ROOT_PATH,
        _PR_GUIDELINES_PATH,
        "Every open PR must meet the following minimum quality criteria",
        "What happens when a PR is converted to draft",
    )

    # Read Gen-AI guidelines (use first paragraph as unique anchor)
    gen_ai_guidelines = _read_file_section(
        AIRFLOW_ROOT_PATH,
        _PR_GUIDELINES_PATH,
        "Generally, it's fine to use Gen-AI tools",
        "Requirement to resolve all conversations",
    )

    # Read code review instructions
    code_review_path = AIRFLOW_ROOT_PATH / _CODE_REVIEW_PATH
    code_review = ""
    if code_review_path.is_file():
        code_review = code_review_path.read_text().strip()

    sections = [_SYSTEM_PROMPT_PREFIX]

    if pr_criteria:
        sections.append(f"## PR Quality Criteria (from contributing docs)\n\n{pr_criteria}")

    if gen_ai_guidelines:
        sections.append(f"## Gen-AI Contribution Guidelines\n\n{gen_ai_guidelines}")

    if code_review:
        sections.append(f"## Code Review Checklist\n\n{code_review}")

    sections.append(_SYSTEM_PROMPT_SUFFIX)
    return "\n\n".join(sections)


_cached_system_prompt: str | None = None


def get_system_prompt() -> str:
    """Get the system prompt, loading from files on first call."""
    global _cached_system_prompt
    if _cached_system_prompt is None:
        _cached_system_prompt = _load_system_prompt()
    return _cached_system_prompt


def _build_user_message(
    pr_number: int,
    pr_title: str,
    pr_body: str,
    check_status_summary: str,
) -> str:
    truncated_body = pr_body[:MAX_PR_BODY_CHARS] if pr_body else "(empty)"
    if pr_body and len(pr_body) > MAX_PR_BODY_CHARS:
        truncated_body += "\n... (truncated)"
    return (
        f"PR #{pr_number}\n"
        f"Title: {pr_title}\n\n"
        f"Description:\n{truncated_body}\n\n"
        f"Check status summary:\n{check_status_summary}\n"
    )


def _extract_json(text: str) -> str:
    """Extract JSON object from LLM response that may contain prose or markdown fences."""
    import re

    # Try to find JSON inside markdown fences first
    fence_match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    if fence_match:
        return fence_match.group(1).strip()

    # Find the first { ... } block (outermost braces)
    start = text.find("{")
    if start == -1:
        return text.strip()
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    # Fallback: return from first brace to end
    return text[start:]


def _parse_response(text: str) -> PRAssessment:
    """Parse LLM JSON response into a PRAssessment."""
    cleaned = _extract_json(text)
    data = json.loads(cleaned)
    violations = [
        Violation(
            category=v.get("category", "unknown"),
            explanation=v.get("explanation", ""),
            severity=v.get("severity", "warning"),
        )
        for v in data.get("violations", [])
    ]
    return PRAssessment(
        should_flag=data.get("should_flag", False),
        violations=violations,
        summary=data.get("summary", ""),
    )


def _resolve_cli_provider(llm_model: str) -> tuple[str, str]:
    """Resolve CLI provider and model name from the llm_model string.

    Format: "provider/model" (e.g. "claude/sonnet", "codex/gpt-5.3-codex").
    """
    if "/" not in llm_model:
        get_console().print(
            f"[error]Invalid model format: {llm_model}. Expected 'provider/model' "
            f"(e.g. 'claude/sonnet', 'codex/gpt-5.3-codex').[/]"
        )
        sys.exit(1)
    provider, model = llm_model.split("/", 1)
    return provider, model


def _check_cli_available(provider: str) -> None:
    """Check that the CLI for the given provider is installed."""
    cli_name = provider
    if shutil.which(cli_name):
        return
    install_hints = {
        "claude": (
            "Install it with: npm install -g @anthropic-ai/claude-code\nThen authenticate with: claude auth"
        ),
        "codex": ("Install it with: npm install -g @openai/codex\nThen authenticate with: codex auth"),
    }
    hint = install_hints.get(provider, f"Install the '{provider}' CLI and ensure it is on your PATH.")
    get_console().print(f"[error]The '{cli_name}' CLI is required for LLM assessment.\n{hint}[/]")
    sys.exit(1)


def _call_claude_cli(model: str, system_prompt: str, user_message: str) -> str:
    """Call Claude via the claude CLI (Claude Code)."""
    result = run_command(
        ["claude", "-p", "--model", model, "--system-prompt", system_prompt, "--output-format", "text"],
        input=user_message,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    if result.returncode != 0:
        error_msg = result.stderr.strip() if result.stderr else "unknown error"
        raise RuntimeError(f"claude CLI failed (exit {result.returncode}): {error_msg}")
    return result.stdout


def _call_codex_cli(model: str, system_prompt: str, user_message: str) -> str:
    """Call OpenAI Codex via the codex CLI.

    The codex CLI has no --system-prompt flag, so we prepend the system prompt
    to the user message.
    """
    combined_prompt = f"{system_prompt}\n\n---\n\n{user_message}"
    result = run_command(
        ["codex", "exec", "--model", model, "--ephemeral", "--sandbox", "read-only", "-"],
        input=combined_prompt,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    if result.returncode != 0:
        error_msg = result.stderr.strip() if result.stderr else "unknown error"
        raise RuntimeError(f"codex CLI failed (exit {result.returncode}): {error_msg}")
    return result.stdout


_CLI_CALLERS: dict[str, Callable[[str, str, str], str]] = {
    "claude": _call_claude_cli,
    "codex": _call_codex_cli,
}


def assess_pr(
    pr_number: int,
    pr_title: str,
    pr_body: str,
    check_status_summary: str,
    llm_model: str,
) -> PRAssessment:
    """Assess a PR using an LLM CLI tool. Returns PRAssessment.

    llm_model must be in "provider/model" format (e.g. "claude/claude-3-opus" or "codex/gpt-5.3-codex").
    """
    provider, model = _resolve_cli_provider(llm_model)
    caller = _CLI_CALLERS.get(provider)
    if not caller:
        get_console().print(f"[error]Unknown CLI provider: {provider}. Use 'claude' or 'codex'.[/]")
        sys.exit(1)

    _check_cli_available(provider)
    system_prompt = get_system_prompt()
    user_message = _build_user_message(pr_number, pr_title, pr_body, check_status_summary)

    try:
        raw = caller(model, system_prompt, user_message)
        return _parse_response(raw)
    except json.JSONDecodeError as e:
        get_console().print(
            f"[warning]Failed to parse LLM response for PR #{pr_number}: {e}\nRaw response:\n{raw}[/]"
        )
        return PRAssessment(should_flag=False, summary="LLM response parse error", error=True)
    except Exception as e:
        get_console().print(f"[warning]LLM error for PR #{pr_number}: {e}. Skipping.[/]")
        return PRAssessment(should_flag=False, summary=f"LLM error: {e}", error=True)
