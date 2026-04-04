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
"""Full-screen TUI display for PR auto-triage and review using Rich."""

from __future__ import annotations

import io
import os
import sys
from enum import Enum
from typing import TYPE_CHECKING

from rich.columns import Columns
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from airflow_breeze.utils.confirm import _has_tty
from airflow_breeze.utils.console import get_theme

if TYPE_CHECKING:
    from airflow_breeze.utils.github import PRAssessment
    from airflow_breeze.utils.pr_models import PRData


class PRCategory(Enum):
    """Category of a PR in the triage view."""

    WORKFLOW_APPROVAL = "WF Approval"
    NON_LLM_ISSUES = "Non-LLM Issues"
    LLM_ERRORS = "LLM Errors"
    LLM_FLAGGED = "LLM Warnings"
    PASSING = "All passed"
    STALE_REVIEW = "Stale Review"
    ALREADY_TRIAGED = "Recently Triaged"
    SKIPPED = "Skipped"


# Category display styles
_CATEGORY_STYLES: dict[PRCategory, str] = {
    PRCategory.WORKFLOW_APPROVAL: "bright_cyan",
    PRCategory.NON_LLM_ISSUES: "yellow",
    PRCategory.LLM_ERRORS: "red",
    PRCategory.LLM_FLAGGED: "yellow",
    PRCategory.PASSING: "green",
    PRCategory.STALE_REVIEW: "yellow",
    PRCategory.ALREADY_TRIAGED: "dim",
    PRCategory.SKIPPED: "dim",
}


_STATIC_CHECK_PATTERNS = (
    "static check",
    "pre-commit",
    "prek",
    "lint",
    "mypy",
    "ruff",
    "black",
    "flake8",
    "pylint",
    "isort",
    "bandit",
    "codespell",
    "yamllint",
    "shellcheck",
)


def _are_only_static_checks(failed_checks: list[str]) -> bool:
    """Return True if all failed checks are static/lint checks (deterministic, not flaky)."""
    if not failed_checks:
        return False
    return all(any(pattern in check.lower() for pattern in _STATIC_CHECK_PATTERNS) for check in failed_checks)


class TUIAction(Enum):
    """Actions the user can take in the TUI."""

    SELECT = "enter"
    UP = "up"
    DOWN = "down"
    PAGE_UP = "page_up"
    PAGE_DOWN = "page_down"
    NEXT_PAGE = "next_page"
    PREV_PAGE = "prev_page"
    QUIT = "q"
    OPEN = "o"
    SHOW_DIFF = "w"
    SKIP = "s"
    NEXT_SECTION = "tab"
    TOGGLE_SELECT = "space"
    BATCH_ACTION = "batch"
    # Direct actions (triage: quick actions; review: code review actions)
    ACTION_DRAFT = "draft"
    ACTION_COMMENT = "comment"
    ACTION_CLOSE = "close"
    ACTION_RERUN = "rerun"
    ACTION_REBASE = "rebase"
    ACTION_READY = "ready"
    ACTION_FLAG = "flag"
    ACTION_LLM = "llm"
    ACTION_AUTHOR_INFO = "author_info"
    SEARCH = "search"


class _FocusPanel(Enum):
    """Which panel currently has focus for keyboard input."""

    PR_LIST = "pr_list"
    DETAIL = "detail"
    DIFF = "diff"


def _get_terminal_size() -> tuple[int, int]:
    """Get terminal size (columns, rows)."""
    try:
        size = os.get_terminal_size()
        return size.columns, size.lines
    except OSError:
        return 120, 40


def _make_tui_console() -> Console:
    """Create a Console instance for TUI rendering."""
    width, _ = _get_terminal_size()
    return Console(
        force_terminal=True,
        color_system="standard",
        width=width,
        theme=get_theme(),
    )


class MouseEvent:
    """Parsed mouse event from terminal escape sequences."""

    def __init__(self, x: int, y: int, button: str):
        self.x = x  # 0-based column
        self.y = y  # 0-based row
        self.button = button  # "click", "scroll_up", "scroll_down", "right_click"


def _enable_mouse() -> None:
    """Enable xterm mouse tracking (SGR mode for coordinates > 223)."""
    if _has_tty():
        sys.stdout.write("\033[?1000h\033[?1006h")  # X10 + SGR extended mode
        sys.stdout.flush()


def _disable_mouse() -> None:
    """Disable xterm mouse tracking."""
    if _has_tty():
        sys.stdout.write("\033[?1006l\033[?1000l")
        sys.stdout.flush()


def _parse_sgr_mouse(seq: str) -> MouseEvent | None:
    """Parse an SGR mouse escape sequence like \\033[<0;col;rowM or \\033[<0;col;rowm."""
    # Format: \033[<Cb;Cx;CyM (press) or \033[<Cb;Cx;Cym (release)
    if not seq.startswith("\033[<"):
        return None
    body = seq[3:]
    is_release = body.endswith("m")
    body = body.rstrip("Mm")
    parts = body.split(";")
    if len(parts) != 3:
        return None
    try:
        cb, cx, cy = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        return None

    # Ignore button release events (only act on press)
    if is_release:
        return None

    x = cx - 1  # convert to 0-based
    y = cy - 1

    if cb == 64:
        return MouseEvent(x, y, "scroll_up")
    if cb == 65:
        return MouseEvent(x, y, "scroll_down")
    if cb == 0:
        return MouseEvent(x, y, "click")
    if cb == 2:
        return MouseEvent(x, y, "right_click")
    return None


def _read_raw_input(*, timeout: float | None = None) -> str | None:
    """Read raw bytes from stdin, handling multi-byte escape sequences.

    If *timeout* is given (seconds), returns ``None`` when no input arrives
    within that window.
    """
    import select
    import termios
    import tty

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        if timeout is not None:
            ready, _, _ = select.select([fd], [], [], timeout)
            if not ready:
                return None
        ch = os.read(fd, 1).decode("utf-8", errors="replace")
        if ch == "\033":
            # Escape sequence — read more bytes with a short timeout
            buf = ch
            while True:
                ready, _, _ = select.select([fd], [], [], 0.05)
                if not ready:
                    break
                b = os.read(fd, 1).decode("utf-8", errors="replace")
                buf += b
                # SGR mouse sequences end with 'M' or 'm'
                if buf.startswith("\033[<") and b in ("M", "m"):
                    break
                # Regular escape sequences end with a letter
                if len(buf) > 2 and b.isalpha():
                    break
                # ~-terminated sequences (PgUp/PgDn)
                if b == "~":
                    break
            return buf
        return ch
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


def _read_tui_key(*, timeout: float | None = None) -> TUIAction | MouseEvent | str | None:
    """Read a keypress or mouse event and map it to a TUIAction, MouseEvent, or raw character.

    Returns ``None`` when *timeout* expires with no input.
    """
    if not _has_tty():
        # No TTY — fall back to line input
        try:
            line = input()
            return line.strip().lower() if line.strip() else TUIAction.SELECT
        except (EOFError, KeyboardInterrupt):
            return TUIAction.QUIT

    ch = _read_raw_input(timeout=timeout)
    if ch is None:
        return None

    # SGR mouse events
    if ch.startswith("\033[<"):
        mouse = _parse_sgr_mouse(ch)
        if mouse:
            return mouse
        return ""

    # Arrow key escape sequences
    if ch == "\x1b[A" or ch == "\x1bOA":
        return TUIAction.UP
    if ch == "\x1b[B" or ch == "\x1bOB":
        return TUIAction.DOWN
    if ch == "\x1b[5~":
        return TUIAction.PAGE_UP
    if ch == "\x1b[6~":
        return TUIAction.PAGE_DOWN
    if ch == " ":
        return TUIAction.TOGGLE_SELECT
    if ch in ("\r", "\n"):
        return TUIAction.SELECT
    if ch == "\t":
        return TUIAction.NEXT_SECTION
    if ch == "\x1b":
        return TUIAction.QUIT
    if ch in ("q", "Q"):
        return TUIAction.QUIT
    if ch in ("o", "O"):
        return TUIAction.OPEN
    if ch in ("e", "E"):
        return TUIAction.SHOW_DIFF
    if ch in ("s", "S"):
        return TUIAction.SKIP
    if ch in ("a", "A"):
        return TUIAction.ACTION_RERUN
    if ch in ("n", "N"):
        return TUIAction.NEXT_PAGE
    if ch in ("p", "P"):
        return TUIAction.PREV_PAGE
    # k/j for vim-style navigation
    if ch == "k":
        return TUIAction.UP
    if ch == "j":
        return TUIAction.DOWN
    # Direct action keys
    if ch == "d":
        return TUIAction.ACTION_DRAFT
    if ch == "c":
        return TUIAction.ACTION_COMMENT
    if ch == "x":
        return TUIAction.ACTION_FLAG
    if ch == "w":
        return TUIAction.ACTION_RERUN
    if ch == "z":
        return TUIAction.ACTION_CLOSE
    if ch == "r":
        return TUIAction.ACTION_REBASE
    if ch == "b":
        return TUIAction.BATCH_ACTION
    if ch == "m":
        return TUIAction.ACTION_READY
    if ch == "f":
        return TUIAction.ACTION_RERUN
    if ch == "l":
        return TUIAction.ACTION_LLM
    if ch == "i":
        return TUIAction.ACTION_AUTHOR_INFO
    if ch == "/":
        return TUIAction.SEARCH
    # Ctrl-C
    if ch == "\x03":
        return TUIAction.QUIT

    # Return raw character for other keys
    return ch if len(ch) == 1 else ""


class PRListEntry:
    """A PR entry in the TUI list with its category and optional metadata."""

    def __init__(self, pr: PRData, category: PRCategory, *, action_taken: str = "", page: int = 0):
        self.pr = pr
        self.category = category
        self.action_taken = action_taken
        self.selected = False
        self.page = page  # batch/page number (0 = initial fetch)
        # LLM review status: "" (not started), "pending", "in_progress", "passed",
        # "flagged", "error", "disabled"
        self.llm_status: str = ""
        # LLM timing
        self.llm_queue_time: float = 0.0  # monotonic time when queued/submitted
        self.llm_submit_time: float = 0.0  # monotonic time when actually started running
        self.llm_duration: float = 0.0  # actual measured LLM execution time in seconds
        self.llm_attempts: int = 0  # number of LLM attempts (including retries)
        # Author scoring (populated when author profile is fetched)
        self.author_scoring: dict | None = None
        # File paths changed by this PR (populated from diff)
        self.file_paths: list[str] | None = None
        # Cross-references found in PR body
        self.cross_refs: list[int] | None = None
        # Overlapping PRs (PR number -> list of shared files)
        self.overlapping_prs: dict[int, list[str]] | None = None


class TriageTUI:
    """Full-screen TUI for PR auto-triage overview."""

    def __init__(
        self,
        title: str = "Auto-Triage",
        *,
        mode_desc: str = "",
        github_repository: str = "",
        selection_criteria: str = "",
        total_matching_prs: int = 0,
    ):
        self.title = title
        self.mode_desc = mode_desc
        self.github_repository = github_repository
        self.selection_criteria = selection_criteria
        self.total_matching_prs = total_matching_prs
        self.entries: list[PRListEntry] = []
        self.cursor: int = 0
        self.scroll_offset: int = 0
        self._visible_rows: int = 0  # set during render
        self._console = _make_tui_console()
        self._sections: dict[PRCategory, list[PRListEntry]] = {}
        self._mouse_enabled: bool = False
        # Layout geometry (row ranges, 0-based) — updated each render
        self._layout_header_end: int = 0
        self._layout_list_start: int = 0
        self._layout_list_end: int = 0
        self._layout_bottom_start: int = 0
        self._layout_bottom_end: int = 0
        # Diff panel state
        self._diff_text: str = ""
        self._diff_lines: list[str] = []
        self._diff_scroll: int = 0
        self._diff_visible_lines: int = 20
        self._diff_pr_number: int | None = None  # track which PR's diff is loaded
        # Detail panel scroll state
        self._detail_lines: list[str] = []
        self._detail_scroll: int = 0
        self._detail_visible_lines: int = 20
        self._detail_pr_number: int | None = None  # track which PR's details are built
        # Assessment data for flagged PRs (PR number → assessment)
        self._assessments: dict[int, PRAssessment] = {}
        # Focus state — which panel receives keyboard navigation
        self._focus: _FocusPanel = _FocusPanel.PR_LIST
        # Track previous cursor to detect PR changes for diff auto-fetch
        self._prev_cursor: int = -1
        # Loading status shown in header (e.g. "Loading more PRs...")
        self._loading_status: str = ""
        # Background activity (type → count or description string)
        self._bg_activity: dict[str, int | str] = {}
        # Whether LLM checks are enabled (affects LLM column display)
        self.llm_enabled: bool = False
        # LLM assessment progress (shown in status panel)
        self.llm_progress: str = ""
        # Total PRs loaded from API (before filtering)
        self.total_loaded_prs: int = 0
        # Whether more pages are available for fetching
        self.has_more_pages: bool = False
        # Spinner frame counter for animated progress
        self._spinner_frame: int = 0
        # Review mode uses green color scheme instead of blue
        self.review_mode: bool = False
        # Color scheme: review = green, triage = blue
        self._accent: str = "bright_blue"
        self._accent_bold: str = "bold bright_blue"
        self._accent_alt: str = "bright_cyan"
        self._accent_alt_bold: str = "bold bright_cyan"

    def set_review_mode(self, enabled: bool) -> None:
        """Set review mode color scheme."""
        self.review_mode = enabled
        if enabled:
            self._accent = "bright_green"
            self._accent_bold = "bold bright_green"
            self._accent_alt = "green"
            self._accent_alt_bold = "bold green"
        else:
            self._accent = "bright_blue"
            self._accent_bold = "bold bright_blue"
            self._accent_alt = "bright_cyan"
            self._accent_alt_bold = "bold bright_cyan"

    def set_entries(
        self,
        entries: list[PRListEntry],
    ) -> None:
        """Set the PR entries to display."""
        self.entries = entries
        self.cursor = 0
        self.scroll_offset = 0
        self._sections.clear()
        for entry in entries:
            self._sections.setdefault(entry.category, []).append(entry)

    def update_entries(self, entries: list[PRListEntry]) -> None:
        """Update entries while preserving the current cursor position."""
        # Remember which PR is selected so we can restore position
        current_pr = (
            self.entries[self.cursor].pr.number if self.entries and self.cursor < len(self.entries) else None
        )
        self.entries = entries
        self._sections.clear()
        for entry in entries:
            self._sections.setdefault(entry.category, []).append(entry)
        # Restore cursor to the same PR if still present
        if current_pr is not None:
            for i, entry in enumerate(entries):
                if entry.pr.number == current_pr:
                    self.cursor = i
                    # Adjust scroll offset so cursor is visible
                    if self._visible_rows > 0:
                        if self.cursor < self.scroll_offset:
                            self.scroll_offset = self.cursor
                        elif self.cursor >= self.scroll_offset + self._visible_rows:
                            self.scroll_offset = self.cursor - self._visible_rows + 1
                    break

    def set_assessments(self, assessments: dict[int, PRAssessment]) -> None:
        """Set assessment data for flagged PRs (PR number → PRAssessment)."""
        self._assessments = assessments

    def _build_header(self, width: int) -> Columns:
        """Build the header as side-by-side main info (left) + status panel (right)."""
        # Count by category
        counts: dict[PRCategory, int] = {}
        for entry in self.entries:
            counts[entry.category] = counts.get(entry.category, 0) + 1

        parts = [f"[bold]{self.title}[/]"]
        if self.github_repository:
            parts[0] += f": [cyan]{self.github_repository}[/]"

        stats_parts = []
        total = len(self.entries)
        stats_parts.append(f"Loaded: [bold]{total}[/]")
        for cat in PRCategory:
            if cat in counts:
                style = _CATEGORY_STYLES.get(cat, "white")
                stats_parts.append(f"[{style}]{cat.value}: {counts[cat]}[/]")
        selected_count = sum(1 for e in self.entries if e.selected)
        if selected_count:
            stats_parts.append(f"[bold green]Selected: {selected_count}[/]")
        if self.mode_desc:
            stats_parts.append(f"Mode: [bold]{self.mode_desc}[/]")

        header_lines = [" | ".join(parts), " | ".join(stats_parts)]
        if self.selection_criteria:
            header_lines.append(f"[dim]Selection: {self.selection_criteria}[/]")
        # Show sorting order: category order (only categories with PRs) then secondary keys
        sorted_cats = sorted(
            (cat for cat in counts),
            key=lambda c: list(PRCategory).index(c),
        )
        cat_order = " → ".join(f"[{_CATEGORY_STYLES.get(c, 'white')}]{c.value}[/]" for c in sorted_cats)
        header_lines.append(f"[dim]Sort: {cat_order}, then by author, PR#[/]")
        header_text = "\n".join(header_lines)
        main_panel = Panel(header_text, border_style=self._accent, padding=(0, 1))

        # Status panel (top-right) — 4-line layout
        import re

        _SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        current_page, total_pages = self._page_info(self._visible_rows) if self._visible_rows > 0 else (1, 1)

        # Line 1: query matching + loaded count
        line1_parts = []
        if self.total_matching_prs:
            line1_parts.append(f"Query matching: [bold]{self.total_matching_prs}[/]")
        if self.total_loaded_prs:
            line1_parts.append(f"Loaded: [bold]{self.total_loaded_prs}[/]")
        line1 = " | ".join(line1_parts)

        # Line 2: page info + more indicator
        more_text = " [dim](more available)[/]" if self.has_more_pages else ""
        line2 = f"Page: [bold]{current_page}[/] / {total_pages}{more_text}"

        # Line 3: LLM assessment numbers (stripped of markup)
        if self.llm_progress:
            clean_llm = re.sub(r"\[/?[^\]]*\]", "", self.llm_progress).strip()
            line3 = f"[dim]{clean_llm}[/]"
        else:
            line3 = ""

        # Line 4: background activity with animated spinner
        active_bg = {k: v for k, v in self._bg_activity.items() if v}
        if active_bg or self._loading_status:
            self._spinner_frame = (self._spinner_frame + 1) % len(_SPINNER)
            spinner_char = _SPINNER[self._spinner_frame]
            if active_bg:
                bg_parts = [f"{k}: {v}" for k, v in active_bg.items()]
                line4 = f"[bold yellow]{spinner_char} Background jobs:[/] [dim]{', '.join(bg_parts)}[/]"
            else:
                line4 = f"[bold yellow]{spinner_char} {self._loading_status}[/]"
        else:
            line4 = "[dim]No background jobs[/]"

        status_text = "\n".join([line1, line2, line3, line4])
        status_panel = Panel(status_text, title="Status", border_style="dim", padding=(0, 1))

        # Status panel is wider; main panel takes the rest
        status_width = max(46, width // 3)
        main_width = max(40, width - status_width - 1)
        main_panel.width = main_width
        status_panel.width = status_width

        return Columns([main_panel, status_panel], expand=True, padding=(0, 0))

    def _page_info(self, visible_rows: int) -> tuple[int, int]:
        """Return (current_page, total_pages) based on visible rows."""
        if not self.entries or visible_rows <= 0:
            return 1, 1
        total_pages = max(1, -(-len(self.entries) // visible_rows))  # ceil division
        current_page = (self.scroll_offset // visible_rows) + 1
        return min(current_page, total_pages), total_pages

    def _build_pr_table(self, visible_rows: int) -> Panel:
        """Build the scrollable PR list table."""
        self._visible_rows = visible_rows
        width, _ = _get_terminal_size()

        # Adjust scroll to keep cursor visible
        if self.cursor < self.scroll_offset:
            self.scroll_offset = self.cursor
        elif self.cursor >= self.scroll_offset + visible_rows:
            self.scroll_offset = self.cursor - visible_rows + 1

        table = Table(
            show_header=True,
            header_style="bold",
            expand=True,
            show_edge=False,
            pad_edge=False,
            box=None,
        )
        table.add_column("", width=2, no_wrap=True)  # cursor indicator
        table.add_column("#", style="cyan", width=6, no_wrap=True)
        table.add_column("PR Classification", width=17, no_wrap=True)
        table.add_column("CI Checks", width=12, no_wrap=True)
        table.add_column("Behind", width=6, justify="right", no_wrap=True)
        table.add_column("Title", ratio=1)
        table.add_column("Author", width=16, no_wrap=True)
        table.add_column("LLM", width=11, no_wrap=True)
        table.add_column("Suggested action", width=16, no_wrap=True)
        table.add_column("Performed action", width=16, no_wrap=True)

        # Calculate end_idx accounting for separator rows between pages
        end_idx = self.scroll_offset
        rows_used = 0
        while end_idx < len(self.entries) and rows_used < visible_rows:
            if end_idx > self.scroll_offset:
                # Check if this entry starts a new page (separator row needed)
                if self.entries[end_idx].page != self.entries[end_idx - 1].page:
                    rows_used += 1  # separator row
                    if rows_used >= visible_rows:
                        break
            rows_used += 1
            end_idx += 1

        prev_page: int | None = None
        for i in range(self.scroll_offset, end_idx):
            entry = self.entries[i]
            pr = entry.pr
            is_selected = i == self.cursor

            # Insert a gray separator row when the page changes
            if prev_page is not None and entry.page != prev_page:
                sep = ["[dim]─[/]"] * 10
                sep[5] = f"[dim]── Page {entry.page + 1} ──[/]"  # Title column
                table.add_row(*sep, style="dim")
            prev_page = entry.page

            # Cursor indicator (show selection checkmark for workflow approval PRs)
            if is_selected and entry.selected:
                cursor_mark = "[bold green]>[/]"
            elif is_selected:
                cursor_mark = "[bold bright_white]>[/]"
            elif entry.selected:
                cursor_mark = "[green]*[/]"
            else:
                cursor_mark = " "

            # PR number
            pr_link = f"[link={pr.url}]#{pr.number}[/link]"
            pr_num = f"[bold cyan]{pr_link}[/]" if is_selected else pr_link

            # Category — show "Wait for LLM" when LLM is pending
            if entry.category == PRCategory.PASSING and entry.llm_status in ("in_progress", "pending"):
                cat_text = "[dim]Wait for LLM[/]"
            else:
                cat_style = _CATEGORY_STYLES.get(entry.category, "white")
                cat_text = f"[{cat_style}]{entry.category.value}[/]"

            # Overall status
            if pr.is_draft:
                status = "[yellow]Draft[/]"
            elif pr.checks_state == "FAILURE" or pr.mergeable == "CONFLICTING":
                status = "[red]Issues[/]"
            elif pr.checks_state in ("UNKNOWN", "NOT_RUN"):
                status = "[yellow]No CI[/]"
            elif pr.checks_state == "PENDING":
                status = "[yellow]Pending[/]"
            else:
                status = "[green]OK[/]"

            # Title with labels - truncate based on available width
            label_suffix = ""
            if pr.labels:
                label_tags = " ".join(f"[dim]\\[{lbl}][/]" for lbl in pr.labels[:3])
                if len(pr.labels) > 3:
                    label_tags += f" [dim]+{len(pr.labels) - 3}[/]"
                label_suffix = " " + label_tags
            max_title = max(20, width - 85)
            title = pr.title[:max_title]
            if len(pr.title) > max_title:
                title += "..."
            if is_selected:
                title = f"[bold]{title}[/]"
            title += label_suffix

            # Author — link to their PRs in this repo
            author_url = f"https://github.com/{self.github_repository}/pulls/{pr.author_login}"
            author = f"[link={author_url}]{pr.author_login[:16]}[/link]"

            # Commits behind
            behind = f"[yellow]{pr.commits_behind}[/]" if pr.commits_behind > 0 else "[green]0[/]"

            # Action taken
            action_text = ""
            if entry.action_taken:
                action_styles = {
                    "drafted": "[yellow]conv. to draft[/]",
                    "commented": "[yellow]commented[/]",
                    "closed": "[red]closed PR[/]",
                    "rebased": "[green]rebased[/]",
                    "rerun": "[green]checks rerun[/]",
                    "workflow approved": "[green]wf approved[/]",
                    "ready": "[green]marked ready[/]",
                    "skipped": "[dim]skipped[/]",
                    "pinged": "[cyan]reviewer pinged[/]",
                    "suspicious": "[red]flagged suspicious[/]",
                }
                action_text = action_styles.get(entry.action_taken, f"[dim]{entry.action_taken}[/]")

            # Row style: greyish background for acted-on PRs, highlighted for cursor
            if entry.action_taken and is_selected:
                row_style = "on grey30"
            elif entry.action_taken:
                row_style = "dim on grey15"
            elif is_selected:
                row_style = "on grey23"
            else:
                row_style = ""

            # LLM review status
            import time as _t

            if entry.llm_status == "in_progress":
                _el = _t.monotonic() - entry.llm_submit_time if entry.llm_submit_time > 0 else 0
                llm_text = f"[yellow]run {_el:.0f}s[/]" if _el > 0 else "[yellow]running[/]"
            elif entry.llm_status == "pending":
                _el = _t.monotonic() - entry.llm_queue_time if entry.llm_queue_time > 0 else 0
                llm_text = f"[dim]queue {_el:.0f}s[/]" if _el > 0 else "[dim]queued[/]"
            elif entry.llm_status == "passed":
                if entry.llm_duration > 0:
                    llm_text = f"[green]{entry.llm_duration:.0f}s ok[/]"
                else:
                    llm_text = "[green]passed[/]"
            elif entry.llm_status == "flagged":
                if entry.llm_duration > 0:
                    llm_text = f"[red]{entry.llm_duration:.0f}s flag[/]"
                else:
                    llm_text = "[red]flagged[/]"
            elif entry.llm_status == "error":
                llm_text = "[red]error[/]"
            elif entry.llm_status == "disabled":
                llm_text = "[dim]disabled[/]"
            else:
                llm_text = "[dim]—[/]"

            # Suggested action
            suggested_text = self.get_suggested_action(entry)

            table.add_row(
                cursor_mark,
                pr_num,
                cat_text,
                status,
                behind,
                title,
                author,
                llm_text,
                suggested_text,
                action_text,
                style=row_style,
            )

        # Add scroll indicators
        scroll_info = ""
        if self.scroll_offset > 0:
            scroll_info += f"  [dim]... {self.scroll_offset} more above[/]"
        remaining_below = len(self.entries) - end_idx
        if remaining_below > 0:
            if scroll_info:
                scroll_info += "  |  "
            scroll_info += f"[dim]{remaining_below} more below ...[/]"

        pos_text = f"[dim]{self.cursor + 1}/{len(self.entries)}[/]"
        current_page, total_pages = self._page_info(visible_rows)
        page_text = f"[dim]Page {current_page}/{total_pages}[/]"

        # Show focus indicator on PR list
        focus_indicator = "[bold bright_white] FOCUS [/]" if self._focus == _FocusPanel.PR_LIST else ""
        title_text = f"PR List  {pos_text}  {page_text}  {focus_indicator}"

        border_style = self._accent_bold if self._focus == _FocusPanel.PR_LIST else self._accent
        return Panel(table, title=title_text, subtitle=scroll_info, border_style=border_style)

    def _build_detail_lines(self, entry: PRListEntry) -> list[str]:
        """Build the full list of detail lines for a PR entry (not truncated)."""
        from airflow_breeze.utils.pr_display import human_readable_age as _human_readable_age

        pr = entry.pr
        lines: list[str] = []

        # PR title and link
        lines.append(f"[bold cyan][link={pr.url}]#{pr.number}[/link][/] [bold]{pr.title}[/]")
        lines.append("")

        # Author
        author_url = f"https://github.com/{self.github_repository}/pulls/{pr.author_login}"
        author_line = (
            f"Author: [bold][link={author_url}]{pr.author_login}[/link][/] ([dim]{pr.author_association}[/])"
        )
        scoring = entry.author_scoring if entry else None
        if scoring:
            tier = scoring.get("contributor_tier", "")
            risk = scoring.get("risk_level", "")
            tier_colors = {
                "established": "green",
                "regular": "cyan",
                "occasional": "yellow",
                "attempted": "red",
                "new": "red",
            }
            risk_colors = {"low": "green", "medium": "yellow", "high": "red"}
            parts = []
            if tier:
                parts.append(f"[{tier_colors.get(tier, 'dim')}]{tier}[/]")
            if risk and risk != "low":
                parts.append(f"risk:[{risk_colors.get(risk, 'dim')}]{risk}[/]")
            repo_rate = scoring.get("repo_merge_rate", 0)
            if repo_rate > 0:
                rc = "green" if repo_rate >= 0.5 else "yellow" if repo_rate >= 0.3 else "red"
                parts.append(f"[{rc}]{repo_rate:.0%}[/] merged")
            if parts:
                author_line += f"  {' | '.join(parts)}"
        lines.append(author_line)

        # Maintainer review decisions
        if pr.review_decisions:
            maintainer_assocs = {"COLLABORATOR", "MEMBER", "OWNER"}
            maintainer_reviews = [
                r for r in pr.review_decisions if r.reviewer_association in maintainer_assocs
            ]
            if maintainer_reviews:
                approved = [r for r in maintainer_reviews if r.state == "APPROVED"]
                changes_requested = [r for r in maintainer_reviews if r.state == "CHANGES_REQUESTED"]
                review_parts: list[str] = []
                if approved:
                    names = ", ".join(r.reviewer_login for r in approved)
                    review_parts.append(f"[green]{len(approved)} approved[/] ({names})")
                if changes_requested:
                    names = ", ".join(r.reviewer_login for r in changes_requested)
                    review_parts.append(f"[red]{len(changes_requested)} changes requested[/] ({names})")
                lines.append(f"Maintainer reviews: {' | '.join(review_parts)}")

        # Timestamps
        lines.append(
            f"Created: {_human_readable_age(pr.created_at)}  |  Updated: {_human_readable_age(pr.updated_at)}"
        )

        # Status info
        lines.append("")
        status_parts: list[str] = []
        if pr.is_draft:
            status_parts.append("[yellow]Draft[/]")
        if pr.mergeable == "CONFLICTING":
            status_parts.append("[red]Merge conflicts[/]")
        elif pr.mergeable == "MERGEABLE":
            status_parts.append("[green]Mergeable[/]")
        if pr.commits_behind > 0:
            status_parts.append(
                f"[yellow]{pr.commits_behind} commit{'s' if pr.commits_behind != 1 else ''} behind[/]"
            )
        if pr.checks_state == "FAILURE":
            status_parts.append(f"[red]CI: Failing ({len(pr.failed_checks)} checks)[/]")
        elif pr.checks_state == "SUCCESS":
            status_parts.append("[green]CI: Passing[/]")
        elif pr.checks_state == "PENDING":
            status_parts.append("[yellow]CI: Pending[/]")
        elif pr.checks_state in ("NOT_RUN", "UNKNOWN"):
            status_parts.append("[yellow]CI: Not run[/]")
        if status_parts:
            lines.append(" | ".join(status_parts))

        # Failed checks
        if pr.failed_checks:
            lines.append("")
            lines.append("[red]Failed checks:[/]")
            for check in pr.failed_checks[:8]:
                lines.append(f"  [red]- {check}[/]")
            if len(pr.failed_checks) > 8:
                lines.append(f"  [dim]... and {len(pr.failed_checks) - 8} more[/]")

        # PR Classification
        lines.append("")
        if entry.category == PRCategory.PASSING and entry.llm_status in ("in_progress", "pending"):
            lines.append("PR Classification: [dim]Wait for LLM[/]")
        else:
            cat_style = _CATEGORY_STYLES.get(entry.category, "white")
            lines.append(f"PR Classification: [{cat_style}]{entry.category.value}[/]")

        # LLM review status with timing
        import time as _time

        from airflow_breeze.utils.pr_display import fmt_duration as _fmt_dur

        if entry.llm_status == "in_progress":
            elapsed = _time.monotonic() - entry.llm_submit_time if entry.llm_submit_time > 0 else 0
            queue_wait = entry.llm_submit_time - entry.llm_queue_time if entry.llm_queue_time > 0 else 0
            parts = ["[yellow]Wait for LLM[/]"]
            if elapsed > 0:
                parts.append(f"running for {_fmt_dur(elapsed)}")
            if queue_wait > 1:
                parts.append(f"queued for {_fmt_dur(queue_wait)}")
            llm_detail = " — ".join(parts)
        elif entry.llm_status == "pending":
            if entry.llm_queue_time > 0:
                queued_for = _time.monotonic() - entry.llm_queue_time
                llm_detail = f"[dim]Queued — waiting for LLM thread ({_fmt_dur(queued_for)})[/]"
            else:
                llm_detail = "[dim]Queued — waiting for LLM thread[/]"
        elif entry.llm_status in ("passed", "flagged", "error"):
            _labels = {
                "passed": "[green]Passed — no issues found[/]",
                "flagged": "[red]Flagged — issues found[/]",
                "error": "[red]Error — LLM assessment failed[/]",
            }
            llm_detail = _labels[entry.llm_status]
            timing_parts = []
            if entry.llm_duration > 0:
                timing_parts.append(f"took {_fmt_dur(entry.llm_duration)}")
            if entry.llm_submit_time > 0 and entry.llm_queue_time > 0:
                _qw = entry.llm_submit_time - entry.llm_queue_time
                if _qw > 1:
                    timing_parts.append(f"queued {_fmt_dur(_qw)}")
            if entry.llm_attempts > 1:
                timing_parts.append(f"{entry.llm_attempts} attempts")
            if timing_parts:
                llm_detail += f" [dim]({', '.join(timing_parts)})[/]"
        elif entry.llm_status == "disabled":
            llm_detail = "[dim]Disabled[/]"
        else:
            llm_detail = "[dim]Not started[/]"
        lines.append(f"LLM Review: {llm_detail}")

        # Labels
        if pr.labels:
            lines.append("")
            label_text = ", ".join(f"[dim]{lbl}[/]" for lbl in pr.labels[:8])
            if len(pr.labels) > 8:
                label_text += f" (+{len(pr.labels) - 8} more)"
            lines.append(f"Labels: {label_text}")

        # Cross-references
        if entry.cross_refs:
            lines.append("")
            ref_links = [
                f"[link=https://github.com/{self.github_repository}/issues/{n}]#{n}[/link]"
                for n in entry.cross_refs[:10]
            ]
            lines.append(f"References: {', '.join(ref_links)}")

        # Overlapping PRs (other open PRs touching the same files)
        if entry.overlapping_prs:
            lines.append("")
            lines.append(f"[yellow]Overlapping PRs ({len(entry.overlapping_prs)}):[/]")
            for opr_num, shared_files in list(entry.overlapping_prs.items())[:5]:
                opr_link = (
                    f"[link=https://github.com/{self.github_repository}/pull/{opr_num}]#{opr_num}[/link]"
                )
                files_text = ", ".join(f.rsplit("/", 1)[-1] for f in shared_files[:3])
                if len(shared_files) > 3:
                    files_text += f" +{len(shared_files) - 3} more"
                lines.append(f"  {opr_link}: {files_text}")

        # Assessment / flagging details (summary and violations)
        assessment = self._assessments.get(pr.number)
        if assessment:
            violations = getattr(assessment, "violations", [])
            summary = getattr(assessment, "summary", "")
            should_report = getattr(assessment, "should_report", False)
            if summary:
                lines.append("")
                lines.append(f"[dim]Summary: {summary}[/]")
            if violations:
                lines.append("")
                if entry.category == PRCategory.LLM_ERRORS:
                    flag_label = "LLM Errors"
                    color = "red"
                elif entry.category == PRCategory.LLM_FLAGGED:
                    flag_label = "LLM Warnings"
                    color = "yellow"
                else:
                    flag_label = "Non-LLM Issues"
                    color = "yellow"
                lines.append(f"[bold {color}]── {flag_label} ──[/]")
                if should_report:
                    lines.append(
                        "[yellow]*** May warrant reporting to GitHub "
                        "(possible spam/injection/ToS violation) ***[/]"
                    )
                for v in violations:
                    sev_color = "red" if v.severity == "error" else "yellow"
                    lines.append(f"  [{sev_color}][{v.severity.upper()}][/] {v.category}")
                    explanation = v.explanation
                    while explanation:
                        lines.append(f"    [dim]{explanation[:70]}[/]")
                        explanation = explanation[70:]

        # Unresolved threads
        if pr.unresolved_threads:
            lines.append("")
            lines.append(f"[yellow]Unresolved review threads: {len(pr.unresolved_threads)}[/]")
            for t in pr.unresolved_threads[:5]:
                body_preview = t.comment_body[:80].replace("\n", " ")
                if len(t.comment_body) > 80:
                    body_preview += "..."
                lines.append(f"  [dim]@{t.reviewer_login}:[/] {body_preview}")
            if len(pr.unresolved_threads) > 5:
                lines.append(f"  [dim]... and {len(pr.unresolved_threads) - 5} more[/]")

        # Action taken
        if entry.action_taken:
            lines.append(f"Action: [bold]{entry.action_taken}[/]")

        return lines

    def scroll_detail(self, delta: int, visible_lines: int = 20) -> None:
        """Scroll the detail panel by delta lines."""
        if not self._detail_lines:
            return
        max_scroll = max(0, len(self._detail_lines) - visible_lines)
        self._detail_scroll = max(0, min(max_scroll, self._detail_scroll + delta))

    def _build_detail_panel(self, panel_height: int) -> Panel:
        """Build the scrollable detail panel for the currently selected PR."""
        is_focused = self._focus == _FocusPanel.DETAIL
        focus_indicator = "[bold bright_white] FOCUS [/]" if is_focused else ""

        if not self.entries:
            return Panel(
                "[dim]No PRs to display[/]",
                title=f"Details  {focus_indicator}",
                border_style="dim",
                height=panel_height,
            )

        entry = self.entries[self.cursor]

        # Rebuild detail lines when cursor moves to a different PR
        if self._detail_pr_number != entry.pr.number:
            self._detail_lines = self._build_detail_lines(entry)
            self._detail_scroll = 0
            self._detail_pr_number = entry.pr.number

        # Visible lines within the panel (subtract borders)
        visible_lines = max(1, panel_height - 2)
        self._detail_visible_lines = visible_lines

        # Clamp scroll
        max_scroll = max(0, len(self._detail_lines) - visible_lines)
        self._detail_scroll = min(self._detail_scroll, max_scroll)

        # Slice to visible window
        end = min(self._detail_scroll + visible_lines, len(self._detail_lines))
        visible = self._detail_lines[self._detail_scroll : end]

        # Pad with empty lines so the panel always fills the allocated height
        while len(visible) < visible_lines:
            visible.append("")

        content = "\n".join(visible)

        # Scroll info
        scrollable = len(self._detail_lines) > visible_lines
        scroll_parts: list[str] = []
        if self._detail_scroll > 0:
            scroll_parts.append(f"{self._detail_scroll} above")
        remaining = len(self._detail_lines) - end
        if remaining > 0:
            scroll_parts.append(f"{remaining} below")
        subtitle = f"[dim]{' | '.join(scroll_parts)}[/]" if scroll_parts else ""

        title_extra = "  [dim]scrollable[/]" if scrollable and not is_focused else ""
        border_style = self._accent_alt_bold if is_focused else self._accent_alt
        return Panel(
            content,
            title=f"Details{title_extra}  {focus_indicator}",
            subtitle=subtitle,
            border_style=border_style,
            padding=(0, 1),
            height=panel_height,
        )

    def set_diff(self, pr_number: int, diff_text: str) -> None:
        """Set the diff content for display."""
        self._diff_text = diff_text
        self._diff_lines = diff_text.splitlines()
        self._diff_scroll = 0
        self._diff_pr_number = pr_number

    def needs_diff_fetch(self) -> bool:
        """Check if the current PR needs a diff fetch (cursor moved to new PR)."""
        entry = self.get_selected_entry()
        if not entry:
            return False
        return self._diff_pr_number != entry.pr.number

    def scroll_diff(self, delta: int, visible_lines: int = 20) -> None:
        """Scroll the diff panel by delta lines."""
        if not self._diff_lines:
            return
        max_scroll = max(0, len(self._diff_lines) - visible_lines)
        self._diff_scroll = max(0, min(max_scroll, self._diff_scroll + delta))

    def _build_diff_panel(self, panel_height: int, panel_width: int) -> Panel:
        """Build the scrollable diff panel."""
        is_focused = self._focus == _FocusPanel.DIFF
        focus_indicator = "[bold bright_white] FOCUS [/]" if is_focused else ""

        if not self._diff_text:
            return Panel(
                "[dim]Loading diff...[/]",
                title=f"Diff  {focus_indicator}",
                border_style=self._accent_alt_bold if is_focused else "dim",
                width=panel_width,
                height=panel_height,
            )

        # Visible lines within the panel (subtract borders)
        visible_lines = max(1, panel_height - 2)
        self._diff_visible_lines = visible_lines

        # Clamp scroll
        max_scroll = max(0, len(self._diff_lines) - visible_lines)
        self._diff_scroll = min(self._diff_scroll, max_scroll)

        # Slice the diff text to the visible window
        end = min(self._diff_scroll + visible_lines, len(self._diff_lines))
        visible_text = "\n".join(self._diff_lines[self._diff_scroll : end])

        diff_content = Syntax(visible_text, "diff", theme="monokai", word_wrap=True)

        # Scroll info
        scroll_info_parts = []
        if self._diff_scroll > 0:
            scroll_info_parts.append(f"{self._diff_scroll} lines above")
        remaining = len(self._diff_lines) - end
        if remaining > 0:
            scroll_info_parts.append(f"{remaining} lines below")
        scroll_info = f"[dim]{' | '.join(scroll_info_parts)}[/]" if scroll_info_parts else ""

        pr_num = self._diff_pr_number or "?"
        pos = f"{self._diff_scroll + 1}-{end}/{len(self._diff_lines)}"
        title = f"Diff #{pr_num}  [dim]{pos}[/]  {focus_indicator}"

        border_style = self._accent_alt_bold if is_focused else self._accent_alt
        return Panel(
            diff_content,
            title=title,
            subtitle=scroll_info,
            border_style=border_style,
            padding=(0, 1),
            width=panel_width,
            height=panel_height,
        )

    def get_suggested_action(self, entry: PRListEntry) -> str:
        """Return a short suggested action label for a PR based on its category and state."""
        # Entries previously skipped should be re-evaluated
        if entry.action_taken == "skipped":
            return "[dim]re-evaluate[/]"
        cat = entry.category
        pr = entry.pr

        if self.review_mode:
            return self._get_suggested_action_review(entry, cat, pr)
        return self._get_suggested_action_triage(entry, cat, pr)

    @staticmethod
    def _get_suggested_action_triage(entry: PRListEntry, cat: PRCategory, pr: PRData) -> str:
        """Suggested actions for triage mode."""
        if cat == PRCategory.WORKFLOW_APPROVAL:
            return "[cyan]wf approve[/]"
        if cat == PRCategory.NON_LLM_ISSUES:
            # Mirror the logic from _compute_default_action
            has_conflicts = pr.mergeable == "CONFLICTING"
            failed_count = len(pr.failed_checks)
            has_ci_failures = failed_count > 0
            has_unresolved = pr.unresolved_review_comments > 0
            if has_conflicts and has_ci_failures:
                return "[yellow]draft[/]"
            if has_conflicts:
                return "[yellow]draft[/]"
            if not has_ci_failures and not has_conflicts and has_unresolved:
                return "[yellow]comment[/]"
            if has_ci_failures and _are_only_static_checks(pr.failed_checks):
                return "[yellow]comment[/]"
            if has_ci_failures and failed_count <= 2 and not has_conflicts and not has_unresolved:
                return "[yellow]rerun failed[/]"
            if has_ci_failures:
                return "[yellow]draft[/]"
            return "[yellow]draft/close[/]"
        if cat == PRCategory.LLM_FLAGGED:
            return "[yellow]comment[/]"
        if cat == PRCategory.LLM_ERRORS:
            return "[red]close/draft[/]"
        if cat == PRCategory.PASSING:
            if entry.llm_status in ("in_progress", "pending"):
                return "[dim]—[/]"
            return "[green]mark ready[/]"
        if cat == PRCategory.STALE_REVIEW:
            return "[yellow]ping reviewer[/]"
        if cat == PRCategory.ALREADY_TRIAGED:
            return "[dim]re-evaluate[/]"
        if cat == PRCategory.SKIPPED:
            return "[dim]—[/]"
        return ""

    @staticmethod
    def _get_suggested_action_review(entry: PRListEntry, cat: PRCategory, pr: PRData) -> str:
        """Suggested actions for review mode — focused on detailed code review."""
        if cat == PRCategory.WORKFLOW_APPROVAL:
            return "[cyan]wf approve[/]"
        if cat == PRCategory.NON_LLM_ISSUES:
            has_conflicts = pr.mergeable == "CONFLICTING"
            failed_count = len(pr.failed_checks)
            has_ci_failures = failed_count > 0
            has_unresolved = pr.unresolved_review_comments > 0
            if has_conflicts:
                return "[yellow]needs rebase[/]"
            if has_ci_failures and _are_only_static_checks(pr.failed_checks):
                return "[yellow]fix lint[/]"
            if has_ci_failures and failed_count <= 2 and not has_unresolved:
                return "[yellow]rerun CI[/]"
            if has_ci_failures:
                return "[yellow]fix CI first[/]"
            if has_unresolved:
                return "[yellow]resolve comments[/]"
            return "[yellow]review issues[/]"
        if cat == PRCategory.LLM_FLAGGED:
            return "[yellow]review findings[/]"
        if cat == PRCategory.LLM_ERRORS:
            return "[red]review errors[/]"
        if cat == PRCategory.PASSING:
            if entry.llm_status in ("in_progress", "pending"):
                return "[dim]LLM reviewing…[/]"
            if entry.llm_status == "flagged":
                return "[yellow]review findings[/]"
            return "[green]ready to review[/]"
        if cat == PRCategory.STALE_REVIEW:
            return "[yellow]follow up[/]"
        if cat == PRCategory.ALREADY_TRIAGED:
            return "[dim]re-review[/]"
        if cat == PRCategory.SKIPPED:
            return "[dim]—[/]"
        return ""

    def get_available_actions(self, entry: PRListEntry) -> list[TUIAction]:
        """Return the direct actions available for a PR entry based on its category and mode."""
        if self.review_mode:
            return self._get_available_actions_review(entry)
        return self._get_available_actions_triage(entry)

    @staticmethod
    def _get_available_actions_triage(entry: PRListEntry) -> list[TUIAction]:
        """Available quick actions for triage mode."""
        cat = entry.category
        pr = entry.pr
        if cat in (PRCategory.NON_LLM_ISSUES, PRCategory.LLM_FLAGGED, PRCategory.LLM_ERRORS):
            actions = [TUIAction.ACTION_RERUN, TUIAction.ACTION_REBASE]
            if not pr.is_draft:
                actions.append(TUIAction.ACTION_DRAFT)
            actions.extend([TUIAction.ACTION_COMMENT, TUIAction.ACTION_CLOSE, TUIAction.ACTION_READY])
            actions.append(TUIAction.ACTION_LLM)
            return actions
        if cat == PRCategory.WORKFLOW_APPROVAL:
            return [TUIAction.ACTION_RERUN, TUIAction.ACTION_FLAG]
        if cat == PRCategory.PASSING:
            return [TUIAction.ACTION_READY, TUIAction.ACTION_RERUN, TUIAction.ACTION_LLM]
        if cat == PRCategory.STALE_REVIEW:
            return [TUIAction.ACTION_COMMENT, TUIAction.ACTION_READY, TUIAction.ACTION_LLM]
        if cat == PRCategory.ALREADY_TRIAGED:
            return [TUIAction.ACTION_LLM]
        return []

    @staticmethod
    def _get_available_actions_review(entry: PRListEntry) -> list[TUIAction]:
        """Available quick actions for review mode — focused on code review workflow."""
        cat = entry.category
        pr = entry.pr
        if cat in (PRCategory.NON_LLM_ISSUES, PRCategory.LLM_FLAGGED, PRCategory.LLM_ERRORS):
            actions = [TUIAction.ACTION_LLM, TUIAction.ACTION_COMMENT, TUIAction.ACTION_RERUN]
            if not pr.is_draft:
                actions.append(TUIAction.ACTION_DRAFT)
            actions.append(TUIAction.ACTION_REBASE)
            return actions
        if cat == PRCategory.WORKFLOW_APPROVAL:
            return [TUIAction.ACTION_RERUN, TUIAction.ACTION_FLAG]
        if cat == PRCategory.PASSING:
            return [TUIAction.ACTION_LLM, TUIAction.ACTION_COMMENT, TUIAction.ACTION_RERUN]
        if cat == PRCategory.STALE_REVIEW:
            return [TUIAction.ACTION_LLM, TUIAction.ACTION_COMMENT]
        if cat == PRCategory.ALREADY_TRIAGED:
            return [TUIAction.ACTION_LLM, TUIAction.ACTION_COMMENT]
        return []

    def _build_footer(self, total_width: int) -> Columns:
        """Build the footer as side-by-side Navigation (compact) + Actions (wider) panels."""
        # Navigation panel — fixed content, smaller width
        _FOCUS_CYCLE = {
            _FocusPanel.PR_LIST: "Detail",
            _FocusPanel.DETAIL: "Diff",
            _FocusPanel.DIFF: "PR list",
        }
        next_panel = _FOCUS_CYCLE.get(self._focus, "next")
        if self._focus in (_FocusPanel.DIFF, _FocusPanel.DETAIL):
            nav_lines = [
                "[bold]j/↓[/] Down  [bold]k/↑[/] Up  [bold]PgDn[/] Page",
                f"[bold]Tab[/] → {next_panel}  [bold]/[/] Search  [bold]🖱[/] Scroll",
                "[bold]Esc/q[/] Quit",
            ]
        else:
            nav_lines = [
                "[bold]j/↓[/] Down  [bold]k/↑[/] Up  [bold]q[/] Quit",
                f"[bold]n[/] Next pg  [bold]p[/] Prev pg  [bold]Tab[/] → {next_panel}",
                "[bold]/[/] Search  [bold]🖱[/] Click row / Scroll panels",
            ]
        nav_text = "\n".join(nav_lines)
        nav_panel = Panel(nav_text, title="Nav", border_style="dim", padding=(0, 1))

        # Actions panel — context-sensitive, gets remaining width
        entry = self.get_selected_entry()
        selected_count = sum(1 for e in self.entries if e.selected)
        if entry is not None and entry.action_taken:
            # PR already acted on — show minimal actions
            actions_line = f"[dim]Action: {entry.action_taken}[/]  [bold]o[/] Open"
            direct_line = ""
        elif entry is not None:
            cat = entry.category
            action_parts: list[str] = []
            if self.review_mode:
                action_parts.append("[bold]Enter[/] Detailed review")
            else:
                action_parts.append("[bold]Enter[/] Sequential assessment")
            action_parts.append("[bold]Space[/] Toggle select")
            actions_line = "  ".join(action_parts)

            # Quick actions for individual PR
            direct_parts: list[str] = []
            direct_parts.append("[bold]o[/] Open")
            direct_parts.append("[bold]s[/] Skip")
            direct_parts.append("[bold]i[/] Author")
            available = self.get_available_actions(entry)
            if available:
                if self.review_mode:
                    _ACTION_LABELS: dict[TUIAction, tuple[str, str]] = {
                        TUIAction.ACTION_LLM: ("l", "LLM review"),
                        TUIAction.ACTION_COMMENT: ("c", "Comment"),
                        TUIAction.ACTION_RERUN: ("f", "Rerun CI"),
                        TUIAction.ACTION_DRAFT: ("d", "Draft"),
                        TUIAction.ACTION_REBASE: ("r", "Rebase"),
                        TUIAction.ACTION_FLAG: ("x", "Suspicious"),
                        TUIAction.ACTION_READY: ("m", "Ready"),
                        TUIAction.ACTION_CLOSE: ("z", "Close"),
                    }
                else:
                    _ACTION_LABELS = {
                        TUIAction.ACTION_DRAFT: ("d", "Draft"),
                        TUIAction.ACTION_COMMENT: ("c", "Comment"),
                        TUIAction.ACTION_CLOSE: ("z", "Close"),
                        TUIAction.ACTION_RERUN: ("f", "Rerun failed"),
                        TUIAction.ACTION_REBASE: ("r", "Rebase"),
                        TUIAction.ACTION_READY: ("m", "Ready"),
                        TUIAction.ACTION_FLAG: ("x", "Suspicious"),
                        TUIAction.ACTION_LLM: ("l", "LLM review"),
                    }
                if cat == PRCategory.WORKFLOW_APPROVAL:
                    _ACTION_LABELS[TUIAction.ACTION_RERUN] = ("a", "Approve")
                for act in available:
                    lbl = _ACTION_LABELS.get(act)
                    if lbl:
                        if act == TUIAction.ACTION_LLM and not self.llm_enabled:
                            direct_parts.append(f"[dim]{lbl[0]} {lbl[1]}[/]")
                        else:
                            direct_parts.append(f"[bold]{lbl[0]}[/] {lbl[1]}")
            quick_label = "Review action:" if self.review_mode else "Quick action:"
            direct_line = f"[dim]{quick_label}[/] " + "  ".join(direct_parts)

            # Batch action line — only shown when PRs are selected
            if selected_count:
                import re as _re

                selected_by_suggestion: dict[str, int] = {}
                for e in self.entries:
                    if e.selected:
                        raw = self.get_suggested_action(e)
                        clean = _re.sub(r"\[/?[^\]]*\]", "", raw)
                        selected_by_suggestion[clean] = selected_by_suggestion.get(clean, 0) + 1
                batch_parts = [f"{act} ({cnt})" for act, cnt in selected_by_suggestion.items()]
                actions_line += f"  [bold green]b[/] [green]Batch action: {', '.join(batch_parts)}[/]"
        else:
            actions_line = ""
            direct_line = ""

        action_lines = [actions_line, direct_line, ""]  # 3 lines for stable height
        actions_text = "\n".join(action_lines)
        actions_panel = Panel(actions_text, title="Actions", border_style=self._accent, padding=(0, 1))

        # Navigation panel needs ~42 chars for longest line, actions gets the rest
        nav_width = 42
        actions_width = max(40, total_width - nav_width - 1)
        nav_panel.width = nav_width
        actions_panel.width = actions_width

        return Columns([nav_panel, actions_panel], expand=True, padding=(0, 0))

    def _build_bottom_panels(self, bottom_height: int, total_width: int) -> Columns:
        """Build the side-by-side detail + diff panels with equal fixed width."""
        panel_width = max(30, total_width // 2)

        detail_panel = self._build_detail_panel(bottom_height)
        diff_panel = self._build_diff_panel(bottom_height, panel_width)

        # Force detail panel to the same fixed width as the diff panel
        detail_panel.width = panel_width

        return Columns(
            [detail_panel, diff_panel],
            expand=True,
            equal=True,
            padding=(0, 0),
        )

    def enable_mouse(self) -> None:
        """Enable mouse tracking for the TUI."""
        if not self._mouse_enabled:
            _enable_mouse()
            self._mouse_enabled = True

    def disable_mouse(self) -> None:
        """Disable mouse tracking (must be called before leaving TUI)."""
        if self._mouse_enabled:
            _disable_mouse()
            self._mouse_enabled = False

    def render(self) -> None:
        """Render the full-screen TUI using a single buffered write to avoid flicker."""
        self.enable_mouse()
        width, height = _get_terminal_size()

        # Build everything into a buffer console first, then output at once
        buf = io.StringIO()
        buf_console = Console(
            file=buf,
            force_terminal=True,
            color_system="standard",
            width=width,
            theme=get_theme(),
        )

        # Calculate layout sizes
        # Header is side-by-side: main panel (3-4 content + 2 border) + status panel (4 content + 2 border)
        header_height = 6  # status panel always has 4 content lines + 2 border
        footer_height = 5  # nav/actions panels + border
        available = height - header_height - footer_height - 2

        # PR list gets top half, bottom panels get bottom half (equal split)
        bottom_height = max(5, available // 2)
        list_height = max(5, available - bottom_height)
        visible_rows = list_height - 3

        # Store layout geometry for mouse hit-testing (0-based row numbers)
        self._layout_header_end = header_height
        self._layout_list_start = header_height
        # PR table has 2 header rows (border + column headers) before data rows
        self._layout_list_end = header_height + list_height
        self._layout_bottom_start = self._layout_list_end
        self._layout_bottom_end = self._layout_list_end + bottom_height

        header = self._build_header(width)
        pr_table = self._build_pr_table(visible_rows)
        bottom = self._build_bottom_panels(bottom_height, width)
        footer = self._build_footer(width)

        buf_console.print(header)
        buf_console.print(pr_table, height=list_height)
        buf_console.print(bottom, height=bottom_height)
        buf_console.print(footer)

        # Single atomic write: move cursor to top-left, overwrite content, then
        # clear any leftover lines *after* the new content so the screen never
        # goes blank between frames.
        output = buf.getvalue()
        self._console = _make_tui_console()
        frame = f"\033[H{output}\033[J"  # home, content, clear-below
        self._console.file.write(frame)
        self._console.file.flush()

    def get_selected_entry(self) -> PRListEntry | None:
        """Return the currently selected PR entry."""
        if self.entries and 0 <= self.cursor < len(self.entries):
            return self.entries[self.cursor]
        return None

    def get_selected_entries(self) -> list[PRListEntry]:
        """Return all PRs that have been selected (toggled) for batch actions."""
        return [e for e in self.entries if e.selected]

    def _is_skippable(self, index: int) -> bool:
        """Return True if the entry at index should be skipped during navigation."""
        if not (0 <= index < len(self.entries)):
            return False
        entry = self.entries[index]
        # Never skip triaged PRs — they should remain navigable for re-evaluation
        if entry.category == PRCategory.ALREADY_TRIAGED:
            return False
        # Don't skip entries marked as "skipped" — they should be navigable for re-evaluation
        if entry.action_taken == "skipped":
            return False
        return bool(entry.action_taken)

    def _find_next_active(self, start: int, direction: int) -> int:
        """Find the next non-acted-on entry from start in the given direction (+1 or -1).

        Falls back to start if all entries are acted on.
        """
        idx = start
        steps = 0
        while 0 <= idx < len(self.entries) and steps < len(self.entries):
            if not self._is_skippable(idx):
                return idx
            idx += direction
            steps += 1
        # All entries acted on — just clamp to bounds
        return max(0, min(len(self.entries) - 1, start))

    def move_cursor(self, delta: int) -> None:
        """Move the cursor by delta positions, skipping acted-on entries."""
        if not self.entries:
            return
        direction = 1 if delta > 0 else -1
        remaining = abs(delta)
        pos = self.cursor
        while remaining > 0:
            pos += direction
            if pos < 0 or pos >= len(self.entries):
                break
            if not self._is_skippable(pos):
                remaining -= 1
        pos = max(0, min(len(self.entries) - 1, pos))
        # If we landed on an acted-on entry, find the nearest active one
        if self._is_skippable(pos):
            pos = self._find_next_active(pos, direction)
        self.cursor = pos

    def next_page(self) -> None:
        """Move cursor to the start of the next page, skipping acted-on entries."""
        if not self.entries or self._visible_rows <= 0:
            return
        new_offset = self.scroll_offset + self._visible_rows
        if new_offset < len(self.entries):
            self.scroll_offset = new_offset
            self.cursor = self._find_next_active(new_offset, 1)

    def prev_page(self) -> None:
        """Move cursor to the start of the previous page, skipping acted-on entries."""
        if not self.entries or self._visible_rows <= 0:
            return
        new_offset = max(0, self.scroll_offset - self._visible_rows)
        self.scroll_offset = new_offset
        self.cursor = self._find_next_active(new_offset, -1 if new_offset > 0 else 1)

    def mark_action(self, index: int, action: str) -> None:
        """Mark a PR entry with the action taken."""
        if 0 <= index < len(self.entries):
            self.entries[index].action_taken = action

    def cursor_changed(self) -> bool:
        """Check if cursor moved to a different PR since last check. Resets tracking."""
        changed = self.cursor != self._prev_cursor
        self._prev_cursor = self.cursor
        return changed

    def _handle_mouse(self, mouse: MouseEvent) -> tuple[PRListEntry | None, TUIAction | str] | None:
        """Handle a mouse event. Returns (entry, action) or None to re-read input."""
        y = mouse.y
        width, _ = _get_terminal_size()
        half_width = width // 2

        # Scroll wheel in PR list area
        if self._layout_list_start <= y < self._layout_list_end:
            if mouse.button == "scroll_up":
                self.move_cursor(-3)
                return None, TUIAction.UP
            if mouse.button == "scroll_down":
                self.move_cursor(3)
                return None, TUIAction.DOWN
            if mouse.button == "click":
                # Click on a PR row — move cursor to that row and focus the list
                self._focus = _FocusPanel.PR_LIST
                # Data rows start after panel border (1) + table header (1) = 2 rows
                data_row = y - self._layout_list_start - 2
                target_index = self.scroll_offset + data_row
                if 0 <= target_index < len(self.entries):
                    self.cursor = target_index
                return None, TUIAction.UP  # signal a cursor move

        # Scroll wheel / click in bottom panels
        if self._layout_bottom_start <= y < self._layout_bottom_end:
            is_left = mouse.x < half_width  # detail panel
            if is_left:
                # Detail panel
                if mouse.button == "scroll_up":
                    self.scroll_detail(-3, self._detail_visible_lines)
                    return None, TUIAction.UP
                if mouse.button == "scroll_down":
                    self.scroll_detail(3, self._detail_visible_lines)
                    return None, TUIAction.DOWN
                if mouse.button == "click":
                    self._focus = _FocusPanel.DETAIL
                    return None, TUIAction.NEXT_SECTION
            else:
                # Diff panel
                if mouse.button == "scroll_up":
                    self.scroll_diff(-3, self._diff_visible_lines)
                    return None, TUIAction.UP
                if mouse.button == "scroll_down":
                    self.scroll_diff(3, self._diff_visible_lines)
                    return None, TUIAction.DOWN
                if mouse.button == "click":
                    self._focus = _FocusPanel.DIFF
                    return None, TUIAction.NEXT_SECTION

        # Ignore other mouse events
        return None

    def render_author_overlay(self, profile: dict) -> None:
        """Render a full-screen overlay with detailed author information.

        Blocks until the user presses any key to dismiss.
        """
        from rich.console import Console
        from rich.panel import Panel

        width, height = _get_terminal_size()

        login = profile.get("login", "unknown")
        tier = profile.get("contributor_tier", "")
        risk = profile.get("risk_level", "")
        repo_rate = profile.get("repo_merge_rate", 0)
        global_rate = profile.get("global_merge_rate", 0)
        age_days = profile.get("account_age_days", 0)

        tier_colors = {
            "established": "green",
            "regular": "cyan",
            "occasional": "yellow",
            "attempted": "red",
            "new": "red",
        }
        risk_colors = {"low": "green", "medium": "yellow", "high": "red"}

        lines: list[str] = []
        lines.append(f"[bold]Account age:[/] {profile.get('account_age', 'unknown')} ({age_days} days)")
        lines.append("")

        # PR stats table
        repo_total = profile.get("repo_total_prs", 0)
        repo_merged = profile.get("repo_merged_prs", 0)
        repo_closed = profile.get("repo_closed_prs", 0)
        rc = "green" if repo_rate >= 0.5 else "yellow" if repo_rate >= 0.3 else "red"

        global_total = profile.get("global_total_prs", 0)
        global_merged = profile.get("global_merged_prs", 0)
        global_closed = profile.get("global_closed_prs", 0)
        gc = "green" if global_rate >= 0.5 else "yellow" if global_rate >= 0.3 else "red"

        lines.append(
            f"[bold]This repo:[/]  {repo_total} PRs, "
            f"[green]{repo_merged} merged[/], "
            f"[red]{repo_closed} closed[/], "
            f"rate [{rc}]{repo_rate:.0%}[/]"
        )
        lines.append(
            f"[bold]All GitHub:[/] {global_total} PRs, "
            f"[green]{global_merged} merged[/], "
            f"[red]{global_closed} closed[/], "
            f"rate [{gc}]{global_rate:.0%}[/]"
        )
        lines.append("")

        # Scoring
        tc = tier_colors.get(tier, "dim")
        rkc = risk_colors.get(risk, "dim")
        lines.append(f"[bold]Contributor tier:[/] [{tc}]{tier}[/]")
        lines.append(f"[bold]Risk level:[/] [{rkc}]{risk}[/]")
        lines.append("")

        # Contributed repos
        repos = profile.get("contributed_repos", [])
        contrib_total = profile.get("contributed_repos_total", 0)
        if repos:
            lines.append(f"[bold]Contributed to ({contrib_total} repos):[/]")
            for repo in repos:
                stars = repo.get("stars", 0)
                star_text = f" ({stars} stars)" if stars else ""
                lines.append(f"  [link={repo['url']}]{repo['name']}[/link]{star_text}")
        else:
            lines.append("[dim]No public repo contributions found[/]")

        lines.append("")
        lines.append("[dim]Press any key to close[/]")

        author_url = f"https://github.com/{login}"
        panel_width = min(width - 8, 70)
        panel_height = min(height - 6, len(lines) + 4)
        panel = Panel(
            "\n".join(lines),
            title=f"[bold][link={author_url}]{login}[/link][/] — Contributor Profile",
            border_style=self._accent_bold,
            height=panel_height,
            width=panel_width,
        )

        # Render the panel into a string buffer
        buf = io.StringIO()
        buf_console = Console(
            file=buf, force_terminal=True, color_system="standard", width=panel_width, theme=get_theme()
        )
        buf_console.print(panel)
        panel_lines = buf.getvalue().split("\n")

        # Calculate centering offsets
        top_offset = max(1, (height - panel_height) // 2)
        left_offset = max(1, (width - panel_width) // 2)
        # Background fill: blank line the full width of the overlay area
        blank_line = " " * (panel_width + 2)

        # Save cursor, hide it, then draw overlay lines at centered position
        out = "\033[?25l"  # hide cursor
        # First paint a solid background block to cover TUI content behind
        for row in range(top_offset, top_offset + panel_height + 1):
            out += f"\033[{row};{left_offset}H{blank_line}"
        # Then draw the panel lines on top
        for i, pline in enumerate(panel_lines):
            if pline:
                row = top_offset + i
                out += f"\033[{row};{left_offset}H {pline}"
        sys.stdout.write(out)
        sys.stdout.flush()

        # Wait for any key to dismiss
        _read_raw_input(timeout=None)
        sys.stdout.write("\033[?25h")  # restore cursor
        sys.stdout.flush()

    def search_jump(self) -> bool:
        """Show a search prompt at the bottom of the screen.

        The user types a PR number or text. Pressing Enter jumps to the first
        matching entry. Pressing Escape cancels. Returns True if the cursor moved.
        """
        width, height = _get_terminal_size()
        prompt = "/ Jump to PR #: "
        query = ""

        while True:
            # Draw prompt on the last row
            display = f"{prompt}{query}_"
            sys.stdout.write(f"\033[{height};1H\033[2K{display}")
            sys.stdout.flush()

            ch = _read_raw_input(timeout=None)
            if ch is None or ch == "\x1b":
                # Escape or timeout — cancel
                break
            if ch in ("\r", "\n"):
                # Enter — search
                break
            if ch in ("\x7f", "\x08"):
                # Backspace
                query = query[:-1]
                continue
            if ch == "\x03":
                # Ctrl-C — cancel
                break
            if len(ch) == 1 and ch.isprintable():
                query += ch

        # Clear the prompt line
        sys.stdout.write(f"\033[{height};1H\033[2K")
        sys.stdout.flush()

        if not query:
            return False

        # Match by PR number only
        try:
            target_num = int(query.lstrip("#"))
        except ValueError:
            return False

        for idx, entry in enumerate(self.entries):
            if entry.pr.number == target_num:
                self.cursor = idx
                # Put the matched entry at the top of the visible list
                self.scroll_offset = idx
                # Switch focus to PR list so the selection is highlighted
                self._focus = _FocusPanel.PR_LIST
                return True

        return False

    def run_interactive(
        self, *, timeout: float | None = None
    ) -> tuple[PRListEntry | None, TUIAction | str | None]:
        """Render and wait for user input. Returns (selected_entry, action).

        When *timeout* is given, returns ``(None, None)`` if no input arrives
        within that window, allowing the caller to perform background work.
        """
        self.render()
        key = _read_tui_key(timeout=timeout)
        if key is None:
            return None, None

        # Handle mouse events
        if isinstance(key, MouseEvent):
            result = self._handle_mouse(key)
            if result is None:
                return None, ""  # ignored mouse event
            return result

        # Tab cycles focus: PR list → Detail → Diff → PR list
        if key == TUIAction.NEXT_SECTION:
            if self._focus == _FocusPanel.PR_LIST:
                self._focus = _FocusPanel.DETAIL
            elif self._focus == _FocusPanel.DETAIL:
                self._focus = _FocusPanel.DIFF
            else:
                self._focus = _FocusPanel.PR_LIST
            return None, key

        # When diff panel has focus, navigation keys scroll the diff
        if self._focus == _FocusPanel.DIFF:
            visible = self._diff_visible_lines
            if key == TUIAction.UP:
                self.scroll_diff(-1, visible)
                return None, key
            if key == TUIAction.DOWN:
                self.scroll_diff(1, visible)
                return None, key
            if key == TUIAction.PAGE_UP:
                self.scroll_diff(-visible, visible)
                return None, key
            if key == TUIAction.PAGE_DOWN:
                self.scroll_diff(visible, visible)
                return None, key
            if key == TUIAction.TOGGLE_SELECT:
                # Space scrolls page-down in diff focus
                self.scroll_diff(visible, visible)
                return None, key
            # Pass through action keys even in diff focus
            if key == TUIAction.QUIT:
                return None, key
            if key == TUIAction.OPEN:
                return self.get_selected_entry(), key
            if key == TUIAction.SELECT:
                return self.get_selected_entry(), key
            if key == TUIAction.SKIP:
                entry = self.get_selected_entry()
                if entry:
                    entry.action_taken = "skipped"
                    self.move_cursor(1)
                return entry, key
            if key == TUIAction.BATCH_ACTION:
                selected = [e for e in self.entries if e.selected]
                if selected:
                    return selected[0], key
                return None, key
            # Author info — always available
            if key == TUIAction.ACTION_AUTHOR_INFO:
                return self.get_selected_entry(), key
            # Direct action keys — pass through if available for this PR
            if isinstance(key, TUIAction) and key.name.startswith("ACTION_"):
                entry = self.get_selected_entry()
                if entry and not entry.action_taken and key in self.get_available_actions(entry):
                    return entry, key
                return None, key
            if key == TUIAction.SEARCH:
                if self.search_jump():
                    return None, TUIAction.UP
                return None, key
            # Ignore other keys in diff focus
            return None, key

        # When detail panel has focus, navigation keys scroll the detail
        if self._focus == _FocusPanel.DETAIL:
            visible = self._detail_visible_lines
            if key == TUIAction.UP:
                self.scroll_detail(-1, visible)
                return None, key
            if key == TUIAction.DOWN:
                self.scroll_detail(1, visible)
                return None, key
            if key == TUIAction.PAGE_UP:
                self.scroll_detail(-visible, visible)
                return None, key
            if key == TUIAction.PAGE_DOWN:
                self.scroll_detail(visible, visible)
                return None, key
            if key == TUIAction.TOGGLE_SELECT:
                # Space scrolls page-down in detail focus
                self.scroll_detail(visible, visible)
                return None, key
            # Pass through action keys even in detail focus
            if key == TUIAction.QUIT:
                return None, key
            if key == TUIAction.OPEN:
                return self.get_selected_entry(), key
            if key == TUIAction.SELECT:
                return self.get_selected_entry(), key
            if key == TUIAction.SKIP:
                entry = self.get_selected_entry()
                if entry:
                    entry.action_taken = "skipped"
                    self.move_cursor(1)
                return entry, key
            if key == TUIAction.BATCH_ACTION:
                selected = [e for e in self.entries if e.selected]
                if selected:
                    return selected[0], key
                return None, key
            # Author info — always available
            if key == TUIAction.ACTION_AUTHOR_INFO:
                return self.get_selected_entry(), key
            # Direct action keys
            if isinstance(key, TUIAction) and key.name.startswith("ACTION_"):
                entry = self.get_selected_entry()
                if entry and not entry.action_taken and key in self.get_available_actions(entry):
                    return entry, key
                return None, key
            if key == TUIAction.SEARCH:
                return None, key
            # Ignore other keys in detail focus
            return None, key

        # PR list has focus — standard navigation
        if key == TUIAction.UP:
            self.move_cursor(-1)
            return None, key
        if key == TUIAction.DOWN:
            self.move_cursor(1)
            return None, key
        if key == TUIAction.PAGE_UP:
            self.prev_page()
            return None, key
        if key == TUIAction.PAGE_DOWN:
            self.next_page()
            return None, key
        if key == TUIAction.NEXT_PAGE:
            self.next_page()
            return None, key
        if key == TUIAction.PREV_PAGE:
            self.prev_page()
            return None, key
        if key == TUIAction.SELECT:
            return self.get_selected_entry(), key
        if key == TUIAction.QUIT:
            return None, key
        if key == TUIAction.OPEN:
            return self.get_selected_entry(), key
        if key == TUIAction.SHOW_DIFF:
            # w key switches focus to diff panel
            self._focus = _FocusPanel.DIFF
            return None, key
        if key == TUIAction.TOGGLE_SELECT:
            entry = self.get_selected_entry()
            if entry and not entry.action_taken:
                entry.selected = not entry.selected
                self.move_cursor(1)
            return None, key
        if key == TUIAction.BATCH_ACTION:
            # Return selected entries for batch approval
            selected = [e for e in self.entries if e.selected]
            if selected:
                return selected[0], key
            return None, key
        if key == TUIAction.SKIP:
            entry = self.get_selected_entry()
            if entry:
                entry.action_taken = "skipped"
                self.move_cursor(1)
            return entry, key
        # Author info — always available
        if key == TUIAction.ACTION_AUTHOR_INFO:
            return self.get_selected_entry(), key
        # Direct action keys — pass through if available for this PR
        if isinstance(key, TUIAction) and key.name.startswith("ACTION_"):
            entry = self.get_selected_entry()
            if entry and not entry.action_taken and key in self.get_available_actions(entry):
                return entry, key
            return None, key
        if key == TUIAction.SEARCH:
            if self.search_jump():
                return None, TUIAction.UP  # signal cursor moved
            return None, key
        # Unknown key — return it for caller to handle
        return self.get_selected_entry(), key
