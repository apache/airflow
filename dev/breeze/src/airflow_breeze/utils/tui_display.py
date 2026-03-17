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
"""Full-screen TUI display for PR auto-triage using Rich."""

from __future__ import annotations

import io
import os
from enum import Enum
from typing import TYPE_CHECKING

from rich.align import Align
from rich.columns import Columns
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from airflow_breeze.utils.confirm import _has_tty
from airflow_breeze.utils.console import get_theme

if TYPE_CHECKING:
    from airflow_breeze.commands.pr_commands import PRData


class PRCategory(Enum):
    """Category of a PR in the triage view."""

    WORKFLOW_APPROVAL = "Needs Workflow"
    FLAGGED = "Flagged"
    LLM_FLAGGED = "LLM Flagged"
    PASSING = "Passing"
    STALE_REVIEW = "Stale Review"
    ALREADY_TRIAGED = "Triaged"
    SKIPPED = "Skipped"


# Category display styles
_CATEGORY_STYLES: dict[PRCategory, str] = {
    PRCategory.WORKFLOW_APPROVAL: "bright_cyan",
    PRCategory.FLAGGED: "red",
    PRCategory.LLM_FLAGGED: "yellow",
    PRCategory.PASSING: "green",
    PRCategory.STALE_REVIEW: "yellow",
    PRCategory.ALREADY_TRIAGED: "dim",
    PRCategory.SKIPPED: "dim",
}


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
    APPROVE_SELECTED = "approve"


class _FocusPanel(Enum):
    """Which panel currently has focus for keyboard input."""

    PR_LIST = "pr_list"
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


def _read_tui_key() -> TUIAction | str:
    """Read a keypress and map it to a TUIAction or return the raw character."""
    if not _has_tty():
        # No TTY — fall back to line input
        try:
            line = input()
            return line.strip().lower() if line.strip() else TUIAction.SELECT
        except (EOFError, KeyboardInterrupt):
            return TUIAction.QUIT

    import click

    ch = click.getchar()

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
    if ch == "q" or ch == "Q":
        return TUIAction.QUIT
    if ch == "o" or ch == "O":
        return TUIAction.OPEN
    if ch == "w" or ch == "W":
        return TUIAction.SHOW_DIFF
    if ch == "s" or ch == "S":
        return TUIAction.SKIP
    if ch == "a" or ch == "A":
        return TUIAction.APPROVE_SELECTED
    if ch == "n" or ch == "N":
        return TUIAction.NEXT_PAGE
    if ch == "p" or ch == "P":
        return TUIAction.PREV_PAGE
    # k/j for vim-style navigation
    if ch == "k":
        return TUIAction.UP
    if ch == "j":
        return TUIAction.DOWN

    # Return raw character for other keys
    return ch if len(ch) == 1 else ""


class PRListEntry:
    """A PR entry in the TUI list with its category and optional metadata."""

    def __init__(self, pr: PRData, category: PRCategory, *, action_taken: str = ""):
        self.pr = pr
        self.category = category
        self.action_taken = action_taken
        self.selected = False


class TriageTUI:
    """Full-screen TUI for PR auto-triage overview."""

    def __init__(
        self,
        title: str = "Auto-Triage",
        *,
        mode_desc: str = "",
        github_repository: str = "",
        selection_criteria: str = "",
    ):
        self.title = title
        self.mode_desc = mode_desc
        self.github_repository = github_repository
        self.selection_criteria = selection_criteria
        self.entries: list[PRListEntry] = []
        self.cursor: int = 0
        self.scroll_offset: int = 0
        self._visible_rows: int = 0  # set during render
        self._console = _make_tui_console()
        self._sections: dict[PRCategory, list[PRListEntry]] = {}
        # Diff panel state
        self._diff_text: str = ""
        self._diff_lines: list[str] = []
        self._diff_scroll: int = 0
        self._diff_visible_lines: int = 20
        self._diff_pr_number: int | None = None  # track which PR's diff is loaded
        # Focus state — which panel receives keyboard navigation
        self._focus: _FocusPanel = _FocusPanel.PR_LIST
        # Track previous cursor to detect PR changes for diff auto-fetch
        self._prev_cursor: int = -1

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

    def _build_header(self, width: int) -> Panel:
        """Build the header panel with title and stats."""
        # Count by category
        counts: dict[PRCategory, int] = {}
        for entry in self.entries:
            counts[entry.category] = counts.get(entry.category, 0) + 1

        parts = [f"[bold]{self.title}[/]"]
        if self.github_repository:
            parts[0] += f": [cyan]{self.github_repository}[/]"

        stats_parts = []
        total = len(self.entries)
        stats_parts.append(f"Total: [bold]{total}[/]")
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
        return Panel(header_text, border_style="bright_blue", padding=(0, 1))

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
        table.add_column("Category", width=15, no_wrap=True)
        table.add_column("Status", width=8, no_wrap=True)
        table.add_column("Title", ratio=1)
        table.add_column("Author", width=16, no_wrap=True)
        table.add_column("CI", width=8, no_wrap=True)
        table.add_column("Behind", width=6, justify="right", no_wrap=True)
        table.add_column("Action", width=10, no_wrap=True)

        end_idx = min(self.scroll_offset + visible_rows, len(self.entries))
        for i in range(self.scroll_offset, end_idx):
            entry = self.entries[i]
            pr = entry.pr
            is_selected = i == self.cursor

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
            pr_num = f"[bold cyan]#{pr.number}[/]" if is_selected else f"#{pr.number}"

            # Category
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

            # Title - truncate based on available width
            max_title = max(20, width - 85)
            title = pr.title[:max_title]
            if len(pr.title) > max_title:
                title += "..."
            if is_selected:
                title = f"[bold]{title}[/]"

            # Author
            author = pr.author_login[:16]

            # CI status
            if pr.checks_state == "FAILURE":
                ci = "[red]Fail[/]"
            elif pr.checks_state == "PENDING":
                ci = "[yellow]Pend[/]"
            elif pr.checks_state in ("UNKNOWN", "NOT_RUN"):
                ci = "[yellow]NotRun[/]"
            elif pr.checks_state == "SUCCESS":
                ci = "[green]Pass[/]"
            else:
                ci = f"[dim]{pr.checks_state[:6]}[/]"

            # Commits behind
            behind = f"[yellow]{pr.commits_behind}[/]" if pr.commits_behind > 0 else "[green]0[/]"

            # Action taken
            action_text = ""
            if entry.action_taken:
                action_styles = {
                    "drafted": "[yellow]drafted[/]",
                    "commented": "[yellow]commented[/]",
                    "closed": "[red]closed[/]",
                    "rebased": "[green]rebased[/]",
                    "rerun": "[green]rerun[/]",
                    "approved": "[green]approved[/]",
                    "ready": "[green]ready[/]",
                    "skipped": "[dim]skipped[/]",
                    "pinged": "[cyan]pinged[/]",
                }
                action_text = action_styles.get(entry.action_taken, f"[dim]{entry.action_taken}[/]")

            # Row style for selected
            row_style = "on grey23" if is_selected else ""

            table.add_row(
                cursor_mark,
                pr_num,
                cat_text,
                status,
                title,
                author,
                ci,
                behind,
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

        border_style = "bold bright_blue" if self._focus == _FocusPanel.PR_LIST else "bright_blue"
        return Panel(table, title=title_text, subtitle=scroll_info, border_style=border_style)

    def _build_detail_panel(self, panel_height: int) -> Panel:
        """Build the detail panel for the currently selected PR."""
        if not self.entries:
            return Panel("[dim]No PRs to display[/]", title="Details", border_style="dim")

        entry = self.entries[self.cursor]
        pr = entry.pr

        lines = []
        # PR title and link
        lines.append(f"[bold cyan]#{pr.number}[/] [bold]{pr.title}[/]")
        lines.append(f"[link={pr.url}]{pr.url}[/link]")
        lines.append("")

        # Author
        lines.append(f"Author: [bold]{pr.author_login}[/] ([dim]{pr.author_association}[/])")

        # Timestamps
        from airflow_breeze.commands.pr_commands import _human_readable_age

        lines.append(
            f"Created: {_human_readable_age(pr.created_at)}  |  Updated: {_human_readable_age(pr.updated_at)}"
        )

        # Status info
        lines.append("")
        status_parts = []
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
            for check in pr.failed_checks[:5]:
                lines.append(f"  [red]- {check}[/]")
            if len(pr.failed_checks) > 5:
                lines.append(f"  [dim]... and {len(pr.failed_checks) - 5} more[/]")

        # Labels
        if pr.labels:
            lines.append("")
            label_text = ", ".join(f"[dim]{lbl}[/]" for lbl in pr.labels[:5])
            if len(pr.labels) > 5:
                label_text += f" (+{len(pr.labels) - 5} more)"
            lines.append(f"Labels: {label_text}")

        # Unresolved threads
        if pr.unresolved_threads:
            lines.append("")
            lines.append(f"[yellow]Unresolved review threads: {len(pr.unresolved_threads)}[/]")
            for t in pr.unresolved_threads[:3]:
                body_preview = t.comment_body[:80].replace("\n", " ")
                if len(t.comment_body) > 80:
                    body_preview += "..."
                lines.append(f"  [dim]@{t.reviewer_login}:[/] {body_preview}")

        # Category
        lines.append("")
        cat_style = _CATEGORY_STYLES.get(entry.category, "white")
        lines.append(f"Category: [{cat_style}]{entry.category.value}[/]")

        # Action taken
        if entry.action_taken:
            lines.append(f"Action: [bold]{entry.action_taken}[/]")

        # Truncate lines to fit panel height (subtract borders)
        max_lines = max(1, panel_height - 2)
        if len(lines) > max_lines:
            lines = lines[:max_lines]

        content = "\n".join(lines)
        return Panel(content, title="Details", border_style="cyan", padding=(0, 1))

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
                border_style="bold bright_cyan" if is_focused else "dim",
                width=panel_width,
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

        border_style = "bold bright_cyan" if is_focused else "bright_cyan"
        return Panel(
            diff_content,
            title=title,
            subtitle=scroll_info,
            border_style=border_style,
            padding=(0, 1),
            width=panel_width,
        )

    def _build_footer(self) -> Panel:
        """Build the footer panel with context-sensitive available actions."""
        # Navigation keys depend on focus
        if self._focus == _FocusPanel.DIFF:
            nav = (
                "[bold]j/↓[/] Scroll down  [bold]k/↑[/] Scroll up  "
                "[bold]PgDn/Space[/] Page down  [bold]PgUp[/] Page up  "
                "[bold]Tab[/] Switch to PR list"
            )
        else:
            nav = (
                "[bold]j/↓[/] Down  [bold]k/↑[/] Up  "
                "[bold]n[/] Next pg  [bold]p[/] Prev pg  "
                "[bold]Tab[/] Switch to diff"
            )

        # Context-sensitive action keys based on selected PR category
        entry = self.get_selected_entry()
        selected_count = sum(1 for e in self.entries if e.selected)
        if entry is not None:
            cat = entry.category
            action_parts: list[str] = []
            if cat in (PRCategory.FLAGGED, PRCategory.LLM_FLAGGED):
                action_parts.append("[bold]Enter[/] Review flagged PR")
            elif cat == PRCategory.WORKFLOW_APPROVAL:
                action_parts.append("[bold]Space[/] Toggle select")
                action_parts.append("[bold]Enter[/] Review workflow")
                if selected_count:
                    action_parts.append(f"[bold green]a[/] [green]Approve selected ({selected_count})[/]")
                else:
                    action_parts.append("[bold]a[/] Approve selected")
            elif cat == PRCategory.PASSING:
                action_parts.append("[bold]Enter[/] Triage PR")
            elif cat == PRCategory.ALREADY_TRIAGED:
                action_parts.append("[bold]Enter[/] View (triaged)")
            else:
                action_parts.append("[bold]Enter[/] View")
            action_parts.append("[bold]o[/] Open in browser")
            action_parts.append("[bold]s[/] Skip")
            action_parts.append("[bold]q[/] Quit")
            actions = "  ".join(action_parts)
        else:
            actions = "[bold]q[/] Quit"

        footer_text = f"{nav}\n{actions}"
        return Panel(
            Align.center(Text.from_markup(footer_text)),
            border_style="bright_blue",
            padding=(0, 1),
        )

    def _build_bottom_panels(self, bottom_height: int, total_width: int) -> Columns:
        """Build the side-by-side detail + diff panels."""
        # Split width: detail gets ~40%, diff gets ~60%
        detail_width = max(30, int(total_width * 0.4))
        diff_width = max(30, total_width - detail_width - 1)  # -1 for column gap

        detail_panel = self._build_detail_panel(bottom_height)
        diff_panel = self._build_diff_panel(bottom_height, diff_width)

        return Columns(
            [detail_panel, diff_panel],
            expand=True,
            equal=False,
            padding=(0, 0),
        )

    def render(self) -> None:
        """Render the full-screen TUI using a single buffered write to avoid flicker."""
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
        header_height = 6 if self.selection_criteria else 5
        footer_height = 4  # two lines of keys + border
        available = height - header_height - footer_height - 2

        # PR list gets ~50%, bottom panels get ~50%
        list_height = max(5, int(available * 0.5))
        bottom_height = max(5, available - list_height)
        visible_rows = list_height - 3

        header = self._build_header(width)
        pr_table = self._build_pr_table(visible_rows)
        bottom = self._build_bottom_panels(bottom_height, width)
        footer = self._build_footer()

        buf_console.print(header)
        buf_console.print(pr_table, height=list_height)
        buf_console.print(bottom, height=bottom_height)
        buf_console.print(footer)

        # Single atomic write: move cursor to top-left and overwrite
        output = buf.getvalue()
        # Use real console for the actual write
        self._console = _make_tui_console()
        # Move to top of screen and overwrite (avoids clear+redraw flicker)
        self._console.file.write("\033[H")  # cursor to home position
        self._console.file.write("\033[J")  # clear from cursor to end
        self._console.file.write(output)
        self._console.file.flush()

    def get_selected_entry(self) -> PRListEntry | None:
        """Return the currently selected PR entry."""
        if self.entries and 0 <= self.cursor < len(self.entries):
            return self.entries[self.cursor]
        return None

    def get_selected_entries(self) -> list[PRListEntry]:
        """Return all PRs that have been selected (toggled) for batch actions."""
        return [e for e in self.entries if e.selected]

    def move_cursor(self, delta: int) -> None:
        """Move the cursor by delta positions, clamping to bounds."""
        if not self.entries:
            return
        self.cursor = max(0, min(len(self.entries) - 1, self.cursor + delta))

    def next_page(self) -> None:
        """Move cursor to the start of the next page."""
        if not self.entries or self._visible_rows <= 0:
            return
        new_offset = self.scroll_offset + self._visible_rows
        if new_offset < len(self.entries):
            self.scroll_offset = new_offset
            self.cursor = new_offset

    def prev_page(self) -> None:
        """Move cursor to the start of the previous page."""
        if not self.entries or self._visible_rows <= 0:
            return
        new_offset = max(0, self.scroll_offset - self._visible_rows)
        self.scroll_offset = new_offset
        self.cursor = new_offset

    def mark_action(self, index: int, action: str) -> None:
        """Mark a PR entry with the action taken."""
        if 0 <= index < len(self.entries):
            self.entries[index].action_taken = action

    def cursor_changed(self) -> bool:
        """Check if cursor moved to a different PR since last check. Resets tracking."""
        changed = self.cursor != self._prev_cursor
        self._prev_cursor = self.cursor
        return changed

    def run_interactive(self) -> tuple[PRListEntry | None, TUIAction | str]:
        """Render and wait for user input. Returns (selected_entry, action)."""
        self.render()
        key = _read_tui_key()

        # Tab switches focus between PR list and diff panel
        if key == TUIAction.NEXT_SECTION:
            if self._focus == _FocusPanel.PR_LIST:
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
            if key == TUIAction.APPROVE_SELECTED:
                selected = [e for e in self.entries if e.selected]
                if selected:
                    return selected[0], key
                return None, key
            # Ignore other keys in diff focus
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
            if entry and entry.category == PRCategory.WORKFLOW_APPROVAL and not entry.action_taken:
                entry.selected = not entry.selected
                self.move_cursor(1)
            return None, key
        if key == TUIAction.APPROVE_SELECTED:
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
        # Unknown key — return it for caller to handle
        return self.get_selected_entry(), key
