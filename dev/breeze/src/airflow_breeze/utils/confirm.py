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

import os
import sys
from enum import Enum

from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.shared_options import get_forced_answer

STANDARD_TIMEOUT = 10


def _has_tty() -> bool:
    """Check if a TTY is available for single-keypress input."""
    try:
        f = open("/dev/tty")
        f.close()
        return True
    except OSError:
        return False


def _read_char() -> str:
    """Read a single character — uses click.getchar() if TTY is available, else input()."""
    if _has_tty():
        import click

        return click.getchar()
    # No TTY (CI) — fall back to line-buffered input
    from inputimeout import inputimeout

    return inputimeout(prompt="", timeout=None)


class Answer(Enum):
    YES = "y"
    NO = "n"
    QUIT = "q"


def user_confirm(
    message: str,
    timeout: float | None = None,
    default_answer: Answer | None = Answer.NO,
    quit_allowed: bool = True,
    forced_answer: str | None = None,
) -> Answer:
    """Ask the user for confirmation.

    Uses single-keypress input (no Enter required) when a TTY is available.
    Falls back to line-buffered input in CI environments.

    :param message: message to display to the user (should end with the question mark)
    :param timeout: time given user to answer
    :param default_answer: default value returned on timeout. If no default - is set, the timeout is ignored.
    :param quit_allowed: whether quit answer is allowed
    :param forced_answer: explicit override for forced answer (bypasses global --answer)
    """
    allowed_answers = "y/n/q" if quit_allowed else "y/n"
    force = forced_answer or get_forced_answer() or os.environ.get("ANSWER")
    if force:
        print(f"Forced answer for '{message}': {force}")
        if force.upper() in ("Y", "YES"):
            return Answer.YES
        if force.upper() in ("N", "NO"):
            return Answer.NO
        if force.upper() in ("Q", "QUIT") and quit_allowed:
            return Answer.QUIT
        return default_answer or Answer.NO

    if default_answer:
        allowed_answers = allowed_answers.replace(default_answer.value, default_answer.value.upper())

    prompt = f"\n{message} \nPress {allowed_answers}: "
    console_print(prompt, end="")

    try:
        ch = _read_char()
    except (KeyboardInterrupt, EOFError):
        console_print()
        if quit_allowed:
            return Answer.QUIT
        sys.exit(1)

    # Ignore multi-byte escape sequences (arrow keys, etc.)
    if len(ch) > 1:
        console_print()
        if default_answer:
            return default_answer
        return Answer.NO

    console_print(ch)

    if ch.upper() == "Y":
        return Answer.YES
    if ch.upper() == "N":
        return Answer.NO
    if ch.upper() == "Q" and quit_allowed:
        return Answer.QUIT
    # Enter/Return selects the default
    if ch in ("\r", "\n", "") and default_answer:
        return default_answer
    # Any other key — treat as default if available
    if default_answer:
        return default_answer
    return Answer.NO


def confirm_action(
    message: str,
    timeout: float | None = None,
    default_answer: Answer | None = Answer.NO,
    quit_allowed: bool = True,
    abort: bool = False,
) -> bool:
    answer = user_confirm(message, timeout, default_answer, quit_allowed)
    if answer == Answer.YES:
        return True
    if abort:
        sys.exit(1)
    elif answer == Answer.QUIT:
        sys.exit(1)
    return False


class TriageAction(Enum):
    DRAFT = "d"
    COMMENT = "c"
    CLOSE = "x"
    REBASE = "b"
    RERUN = "r"
    PING = "p"
    OPEN = "o"
    SHOW = "w"
    READY = "m"
    SKIP = "s"
    QUIT = "q"


def _show_pr_diff(token: str, github_repository: str, pr_number: int, pr_url: str | None) -> None:
    """Fetch and display a nicely formatted diff for a PR."""
    import requests
    from rich.panel import Panel
    from rich.syntax import Syntax

    console = get_console()
    console.print(f"  Fetching diff for PR #{pr_number}...")
    url = f"https://api.github.com/repos/{github_repository}/pulls/{pr_number}"
    try:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3.diff"},
            timeout=60,
        )
    except Exception as e:
        console.print(f"  [warning]Failed to fetch diff: {e}[/]")
        return

    if response.status_code != 200:
        console.print(
            f"  [warning]Could not fetch diff (HTTP {response.status_code}). "
            f"Review manually at: {pr_url}/files[/]"
        )
        return

    diff_text = response.text
    if not diff_text.strip():
        console.print("  [info]Diff is empty (no file changes).[/]")
        return

    pr_link = f"[link={pr_url}]#{pr_number}[/link]" if pr_url else f"#{pr_number}"
    console.print(
        Panel(
            Syntax(diff_text, "diff", theme="monokai", word_wrap=True),
            title=f"Diff for PR {pr_link}",
            border_style="bright_cyan",
        )
    )

    # Warn about sensitive file changes
    import re

    sensitive_paths: list[str] = []
    for match in re.finditer(r"^diff --git a/\S+ b/(\S+)", diff_text, re.MULTILINE):
        path = match.group(1)
        if path.startswith((".github/", "scripts/")):
            if path not in sensitive_paths:
                sensitive_paths.append(path)
    if sensitive_paths:
        console.print()
        console.print(
            "[bold red]WARNING: This PR contains changes to sensitive files — please review carefully![/]"
        )
        for f in sensitive_paths:
            console.print(f"  [bold red]  - {f}[/]")
        console.print()


def prompt_triage_action(
    message: str,
    default: TriageAction = TriageAction.DRAFT,
    timeout: float | None = None,
    forced_answer: str | None = None,
    exclude: set[TriageAction] | None = None,
    pr_url: str | None = None,
    token: str | None = None,
    github_repository: str | None = None,
    pr_number: int | None = None,
) -> TriageAction:
    """Prompt the user to choose a triage action for a flagged PR.

    Uses single-keypress input (no Enter required) when a TTY is available.
    Falls back to line-buffered input in CI environments.

    :param message: message to display (should describe the PR)
    :param default: default action returned on Enter or timeout
    :param timeout: seconds before auto-selecting default (None = no timeout)
    :param forced_answer: explicit override for forced answer (bypasses global --answer)
    :param pr_url: URL of the PR (used by OPEN action to open in browser)
    :param token: GitHub token (used by SHOW action to fetch diff)
    :param github_repository: GitHub repository (used by SHOW action to fetch diff)
    :param pr_number: PR number (used by SHOW action to fetch diff)
    """
    import webbrowser

    _LABELS = {
        TriageAction.DRAFT: "draft",
        TriageAction.COMMENT: "comment",
        TriageAction.CLOSE: "close",
        TriageAction.REBASE: "rebase",
        TriageAction.RERUN: "rerun checks",
        TriageAction.PING: "ping reviewer",
        TriageAction.OPEN: "open in browser",
        TriageAction.SHOW: "show diff",
        TriageAction.READY: "mark as ready",
        TriageAction.SKIP: "skip",
        TriageAction.QUIT: "quit",
    }

    force = forced_answer or get_forced_answer() or os.environ.get("ANSWER")
    if force:
        print(f"Forced answer for '{message}': {force}")
        upper = force.upper()
        if upper in ("Y", "YES"):
            return default
        if upper in ("N", "NO"):
            return TriageAction.SKIP
        if upper == "Q":
            return TriageAction.QUIT
        for action in TriageAction:
            if upper == action.value.upper():
                return action
        return default

    excluded = exclude or set()
    available_actions = [a for a in TriageAction if a not in excluded]
    action_by_key = {a.value.upper(): a for a in available_actions}

    while True:
        # Build choice display: uppercase the default letter
        # Use escaped brackets so Rich doesn't interpret them as markup tags
        choices = []
        for action in available_actions:
            letter = action.value
            label = _LABELS[action]
            if action == default:
                choices.append(f"\\[{letter.upper()}]{label}")
            else:
                choices.append(f"\\[{letter}]{label}")
        choices_str = " / ".join(choices)

        console_print(f"\n{message}")
        console_print(choices_str + ": ", end="")

        try:
            ch = _read_char()
        except (KeyboardInterrupt, EOFError):
            console_print()
            return TriageAction.QUIT

        # Ignore multi-byte escape sequences (arrow keys, etc.)
        if len(ch) > 1:
            console_print()
            continue

        console_print(ch)

        # Enter/Return or empty string (line-buffered) selects the default
        if ch in ("\r", "\n", ""):
            return default

        matched = action_by_key.get(ch.upper())
        if matched:
            if matched == TriageAction.OPEN:
                if pr_url:
                    webbrowser.open(pr_url)
                    console_print(f"  [info]Opened {pr_url} in browser.[/]")
                else:
                    console_print("  [warning]No PR URL available to open.[/]")
                continue  # re-prompt after opening browser
            if matched == TriageAction.SHOW:
                if token and github_repository and pr_number:
                    _show_pr_diff(token, github_repository, pr_number, pr_url)
                else:
                    get_console().print("  [warning]Diff context not available.[/]")
                continue  # re-prompt after showing diff
            return matched

        valid = "/".join(a.value for a in available_actions)
        console_print(f"  [warning]Invalid key. Press one of: {valid}[/]")
