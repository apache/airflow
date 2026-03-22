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

from dataclasses import dataclass, field

from airflow_breeze.utils.tui_display import (
    MouseEvent,
    PRCategory,
    PRListEntry,
    TriageTUI,
    TUIAction,
    _parse_sgr_mouse,
)


# ---------------------------------------------------------------------------
# Lightweight stand-in for PRData so tests don't depend on pr_commands
# ---------------------------------------------------------------------------
@dataclass
class _FakePR:
    number: int = 1
    title: str = "Test PR"
    body: str = ""
    url: str = "https://github.com/test/repo/pull/1"
    created_at: str = "2026-01-01T00:00:00Z"
    updated_at: str = "2026-01-01T00:00:00Z"
    node_id: str = "PR_1"
    author_login: str = "author"
    author_association: str = "NONE"
    head_sha: str = "abc123"
    base_ref: str = "main"
    check_summary: str = ""
    checks_state: str = "SUCCESS"
    failed_checks: list[str] = field(default_factory=list)
    commits_behind: int = 0
    is_draft: bool = False
    mergeable: str = "MERGEABLE"
    labels: list[str] = field(default_factory=list)
    unresolved_threads: list = field(default_factory=list)
    unresolved_review_comments: int = 0
    has_collaborator_review: bool = False


def _make_entry(
    number: int = 1,
    category: PRCategory = PRCategory.PASSING,
    action_taken: str = "",
    author: str = "author",
    is_draft: bool = False,
    checks_state: str = "SUCCESS",
) -> PRListEntry:
    """Create a PRListEntry with a fake PR for testing."""
    pr = _FakePR(
        number=number,
        author_login=author,
        is_draft=is_draft,
        checks_state=checks_state,
        url=f"https://github.com/test/repo/pull/{number}",
    )
    return PRListEntry(pr, category, action_taken=action_taken)


def _make_tui_with_entries(entries: list[PRListEntry]) -> TriageTUI:
    """Create a TriageTUI with entries pre-set."""
    tui = TriageTUI(title="Test", github_repository="test/repo")
    tui.set_entries(entries)
    return tui


# ===========================================================================
# _parse_sgr_mouse
# ===========================================================================
class TestParseSgrMouse:
    def test_left_click(self):
        mouse = _parse_sgr_mouse("\033[<0;10;5M")
        assert mouse is not None
        assert mouse.x == 9  # 0-based
        assert mouse.y == 4
        assert mouse.button == "click"

    def test_scroll_up(self):
        mouse = _parse_sgr_mouse("\033[<64;1;1M")
        assert mouse is not None
        assert mouse.button == "scroll_up"

    def test_scroll_down(self):
        mouse = _parse_sgr_mouse("\033[<65;1;1M")
        assert mouse is not None
        assert mouse.button == "scroll_down"

    def test_right_click(self):
        mouse = _parse_sgr_mouse("\033[<2;5;10M")
        assert mouse is not None
        assert mouse.button == "right_click"

    def test_release_event_ignored(self):
        # Release events end with lowercase 'm'
        assert _parse_sgr_mouse("\033[<0;10;5m") is None

    def test_invalid_prefix(self):
        assert _parse_sgr_mouse("not a mouse event") is None

    def test_malformed_sequence(self):
        assert _parse_sgr_mouse("\033[<0;10M") is None  # missing third field

    def test_non_numeric_fields(self):
        assert _parse_sgr_mouse("\033[<a;b;cM") is None


# ===========================================================================
# PRListEntry
# ===========================================================================
class TestPRListEntry:
    def test_defaults(self):
        entry = _make_entry()
        assert entry.action_taken == ""
        assert entry.selected is False
        assert entry.page == 0

    def test_custom_values(self):
        pr = _FakePR(number=42)
        entry = PRListEntry(pr, PRCategory.NON_LLM_ISSUES, action_taken="drafted", page=2)
        assert entry.action_taken == "drafted"
        assert entry.page == 2
        assert entry.category == PRCategory.NON_LLM_ISSUES


# ===========================================================================
# TriageTUI — entry management
# ===========================================================================
class TestTUIEntryManagement:
    def test_set_entries(self):
        entries = [_make_entry(i) for i in range(5)]
        tui = _make_tui_with_entries(entries)
        assert len(tui.entries) == 5
        assert tui.cursor == 0
        assert tui.scroll_offset == 0

    def test_set_entries_resets_cursor(self):
        tui = _make_tui_with_entries([_make_entry(i) for i in range(5)])
        tui.cursor = 3
        tui.set_entries([_make_entry(10)])
        assert tui.cursor == 0

    def test_update_entries_preserves_cursor(self):
        entries = [_make_entry(i) for i in range(5)]
        tui = _make_tui_with_entries(entries)
        tui.cursor = 2  # pointing at PR #2

        # Add a new entry at the start — PR #2 should still be selected
        new_entries = [_make_entry(99)] + entries
        tui.update_entries(new_entries)
        assert tui.entries[tui.cursor].pr.number == 2

    def test_update_entries_cursor_fallback(self):
        entries = [_make_entry(i) for i in range(3)]
        tui = _make_tui_with_entries(entries)
        tui.cursor = 1  # PR #1

        # Replace with entries that don't contain PR #1
        tui.update_entries([_make_entry(10), _make_entry(20)])
        # Cursor stays at original index (clamped)
        assert tui.cursor == 1

    def test_get_selected_entry(self):
        entries = [_make_entry(i) for i in range(3)]
        tui = _make_tui_with_entries(entries)
        tui.cursor = 1
        selected = tui.get_selected_entry()
        assert selected is not None
        assert selected.pr.number == 1

    def test_get_selected_entry_empty(self):
        tui = _make_tui_with_entries([])
        assert tui.get_selected_entry() is None

    def test_get_selected_entries(self):
        entries = [_make_entry(i) for i in range(5)]
        entries[1].selected = True
        entries[3].selected = True
        tui = _make_tui_with_entries(entries)
        selected = tui.get_selected_entries()
        assert len(selected) == 2
        assert {e.pr.number for e in selected} == {1, 3}

    def test_mark_action(self):
        entries = [_make_entry(i) for i in range(3)]
        tui = _make_tui_with_entries(entries)
        tui.mark_action(1, "approved")
        assert entries[1].action_taken == "approved"

    def test_mark_action_out_of_range(self):
        tui = _make_tui_with_entries([_make_entry()])
        tui.mark_action(99, "test")  # should not raise

    def test_cursor_changed(self):
        tui = _make_tui_with_entries([_make_entry(i) for i in range(3)])
        # First call — always reports changed (prev was -1)
        assert tui.cursor_changed() is True
        # Same position — no change
        assert tui.cursor_changed() is False
        # Move cursor
        tui.cursor = 2
        assert tui.cursor_changed() is True
        assert tui.cursor_changed() is False


# ===========================================================================
# TriageTUI — navigation (move_cursor, next_page, prev_page)
# ===========================================================================
class TestTUINavigation:
    def test_move_cursor_down(self):
        tui = _make_tui_with_entries([_make_entry(i) for i in range(5)])
        tui.move_cursor(1)
        assert tui.cursor == 1

    def test_move_cursor_up(self):
        tui = _make_tui_with_entries([_make_entry(i) for i in range(5)])
        tui.cursor = 3
        tui.move_cursor(-1)
        assert tui.cursor == 2

    def test_move_cursor_clamps_bottom(self):
        tui = _make_tui_with_entries([_make_entry(i) for i in range(3)])
        tui.move_cursor(10)
        assert tui.cursor == 2

    def test_move_cursor_clamps_top(self):
        tui = _make_tui_with_entries([_make_entry(i) for i in range(3)])
        tui.move_cursor(-5)
        assert tui.cursor == 0

    def test_move_cursor_skips_acted_on(self):
        entries = [_make_entry(i) for i in range(5)]
        entries[1].action_taken = "drafted"
        entries[2].action_taken = "closed"
        tui = _make_tui_with_entries(entries)
        tui.move_cursor(1)  # skip entries 1 and 2
        assert tui.cursor == 3

    def test_move_cursor_skips_multiple_acted_on_backwards(self):
        entries = [_make_entry(i) for i in range(5)]
        entries[2].action_taken = "drafted"
        entries[3].action_taken = "closed"
        tui = _make_tui_with_entries(entries)
        tui.cursor = 4
        tui.move_cursor(-1)  # skip entries 3 and 2
        assert tui.cursor == 1

    def test_move_cursor_all_acted_on(self):
        entries = [_make_entry(i, action_taken="done") for i in range(3)]
        tui = _make_tui_with_entries(entries)
        tui.move_cursor(1)  # should not crash
        assert 0 <= tui.cursor < 3

    def test_move_cursor_empty(self):
        tui = _make_tui_with_entries([])
        tui.move_cursor(1)  # should not crash
        assert tui.cursor == 0

    def test_next_page(self):
        entries = [_make_entry(i) for i in range(20)]
        tui = _make_tui_with_entries(entries)
        tui._visible_rows = 5
        tui.next_page()
        assert tui.scroll_offset == 5
        assert tui.cursor == 5

    def test_prev_page(self):
        entries = [_make_entry(i) for i in range(20)]
        tui = _make_tui_with_entries(entries)
        tui._visible_rows = 5
        tui.scroll_offset = 10
        tui.cursor = 10
        tui.prev_page()
        assert tui.scroll_offset == 5

    def test_next_page_skips_acted_on(self):
        entries = [_make_entry(i) for i in range(20)]
        entries[5].action_taken = "done"
        tui = _make_tui_with_entries(entries)
        tui._visible_rows = 5
        tui.next_page()
        assert tui.cursor == 6  # skips the acted-on entry at index 5


# ===========================================================================
# TriageTUI — _is_skippable / _find_next_active
# ===========================================================================
class TestSkippableLogic:
    def test_is_skippable_true(self):
        entries = [_make_entry(0, action_taken="drafted")]
        tui = _make_tui_with_entries(entries)
        assert tui._is_skippable(0) is True

    def test_is_skippable_false(self):
        entries = [_make_entry(0)]
        tui = _make_tui_with_entries(entries)
        assert tui._is_skippable(0) is False

    def test_is_skippable_out_of_range(self):
        tui = _make_tui_with_entries([_make_entry(0)])
        assert tui._is_skippable(5) is False

    def test_find_next_active_forward(self):
        entries = [
            _make_entry(0, action_taken="done"),
            _make_entry(1, action_taken="done"),
            _make_entry(2),
        ]
        tui = _make_tui_with_entries(entries)
        assert tui._find_next_active(0, 1) == 2

    def test_find_next_active_backward(self):
        entries = [
            _make_entry(0),
            _make_entry(1, action_taken="done"),
            _make_entry(2, action_taken="done"),
        ]
        tui = _make_tui_with_entries(entries)
        assert tui._find_next_active(2, -1) == 0

    def test_find_next_active_all_acted_on(self):
        entries = [_make_entry(i, action_taken="done") for i in range(3)]
        tui = _make_tui_with_entries(entries)
        # Falls back to clamped start position
        result = tui._find_next_active(0, 1)
        assert 0 <= result < 3


# ===========================================================================
# TriageTUI — diff and detail panel state
# ===========================================================================
class TestDiffAndDetailState:
    def test_set_diff(self):
        tui = _make_tui_with_entries([_make_entry()])
        tui.set_diff(42, "line1\nline2\nline3")
        assert tui._diff_pr_number == 42
        assert len(tui._diff_lines) == 3
        assert tui._diff_scroll == 0

    def test_needs_diff_fetch_no_entries(self):
        tui = _make_tui_with_entries([])
        assert tui.needs_diff_fetch() is False

    def test_needs_diff_fetch_same_pr(self):
        tui = _make_tui_with_entries([_make_entry(5)])
        tui.set_diff(5, "diff")
        assert tui.needs_diff_fetch() is False

    def test_needs_diff_fetch_different_pr(self):
        tui = _make_tui_with_entries([_make_entry(5)])
        tui.set_diff(99, "diff")
        assert tui.needs_diff_fetch() is True

    def test_scroll_diff(self):
        tui = _make_tui_with_entries([_make_entry()])
        tui.set_diff(1, "\n".join(f"line{i}" for i in range(100)))
        tui.scroll_diff(10, 20)
        assert tui._diff_scroll == 10
        tui.scroll_diff(-5, 20)
        assert tui._diff_scroll == 5

    def test_scroll_diff_clamps(self):
        tui = _make_tui_with_entries([_make_entry()])
        tui.set_diff(1, "a\nb\nc")
        tui.scroll_diff(-10, 20)
        assert tui._diff_scroll == 0
        tui.scroll_diff(100, 20)
        assert tui._diff_scroll == 0  # 3 lines fits in 20 visible

    def test_scroll_detail(self):
        tui = _make_tui_with_entries([_make_entry()])
        tui._detail_lines = [f"line{i}" for i in range(50)]
        tui.scroll_detail(5, 10)
        assert tui._detail_scroll == 5
        tui.scroll_detail(-2, 10)
        assert tui._detail_scroll == 3

    def test_scroll_detail_clamps(self):
        tui = _make_tui_with_entries([_make_entry()])
        tui._detail_lines = [f"line{i}" for i in range(5)]
        tui.scroll_detail(-10, 20)
        assert tui._detail_scroll == 0


# ===========================================================================
# TriageTUI — _page_info
# ===========================================================================
class TestPageInfo:
    def test_single_page(self):
        tui = _make_tui_with_entries([_make_entry(i) for i in range(3)])
        assert tui._page_info(10) == (1, 1)

    def test_multiple_pages(self):
        tui = _make_tui_with_entries([_make_entry(i) for i in range(25)])
        current, total = tui._page_info(10)
        assert total == 3
        assert current == 1

    def test_page_after_scroll(self):
        tui = _make_tui_with_entries([_make_entry(i) for i in range(25)])
        tui.scroll_offset = 10
        current, total = tui._page_info(10)
        assert current == 2

    def test_empty_entries(self):
        tui = _make_tui_with_entries([])
        assert tui._page_info(10) == (1, 1)

    def test_zero_visible_rows(self):
        tui = _make_tui_with_entries([_make_entry()])
        assert tui._page_info(0) == (1, 1)


# ===========================================================================
# TriageTUI.get_available_actions (triage mode)
# ===========================================================================
class TestGetAvailableActions:
    def setup_method(self):
        self.tui = TriageTUI(title="Test", github_repository="test/repo")

    def test_flagged_non_draft(self):
        entry = _make_entry(category=PRCategory.NON_LLM_ISSUES)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_RERUN in actions
        assert TUIAction.ACTION_DRAFT in actions
        assert TUIAction.ACTION_CLOSE in actions

    def test_flagged_draft(self):
        entry = _make_entry(category=PRCategory.NON_LLM_ISSUES, is_draft=True)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_DRAFT not in actions
        assert TUIAction.ACTION_RERUN in actions

    def test_llm_errors(self):
        entry = _make_entry(category=PRCategory.LLM_ERRORS)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_COMMENT in actions

    def test_llm_warnings(self):
        entry = _make_entry(category=PRCategory.LLM_FLAGGED)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_RERUN in actions

    def test_workflow_approval(self):
        entry = _make_entry(category=PRCategory.WORKFLOW_APPROVAL)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_RERUN in actions
        assert TUIAction.ACTION_READY not in actions

    def test_passing(self):
        entry = _make_entry(category=PRCategory.PASSING)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_READY in actions
        assert TUIAction.ACTION_RERUN in actions

    def test_stale_review(self):
        entry = _make_entry(category=PRCategory.STALE_REVIEW)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_COMMENT in actions
        assert TUIAction.ACTION_READY in actions

    def test_already_triaged(self):
        entry = _make_entry(category=PRCategory.ALREADY_TRIAGED)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_LLM in actions

    def test_skipped_no_actions(self):
        entry = _make_entry(category=PRCategory.SKIPPED)
        assert self.tui.get_available_actions(entry) == []


# ===========================================================================
# TriageTUI.get_available_actions (review mode)
# ===========================================================================
class TestGetAvailableActionsReviewMode:
    def setup_method(self):
        self.tui = TriageTUI(title="Test", github_repository="test/repo")
        self.tui.set_review_mode(True)

    def test_flagged_non_draft(self):
        entry = _make_entry(category=PRCategory.NON_LLM_ISSUES)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_LLM in actions
        assert TUIAction.ACTION_COMMENT in actions
        assert TUIAction.ACTION_RERUN in actions
        assert TUIAction.ACTION_DRAFT in actions
        # Close is not a primary review action
        assert TUIAction.ACTION_CLOSE not in actions

    def test_flagged_draft(self):
        entry = _make_entry(category=PRCategory.NON_LLM_ISSUES, is_draft=True)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_DRAFT not in actions
        assert TUIAction.ACTION_LLM in actions

    def test_passing_llm_first(self):
        entry = _make_entry(category=PRCategory.PASSING)
        actions = self.tui.get_available_actions(entry)
        assert actions[0] == TUIAction.ACTION_LLM
        assert TUIAction.ACTION_COMMENT in actions

    def test_already_triaged_has_actions(self):
        entry = _make_entry(category=PRCategory.ALREADY_TRIAGED)
        actions = self.tui.get_available_actions(entry)
        assert TUIAction.ACTION_LLM in actions
        assert TUIAction.ACTION_COMMENT in actions


# ===========================================================================
# TriageTUI.get_suggested_action (review mode)
# ===========================================================================
class TestGetSuggestedActionReviewMode:
    def setup_method(self):
        self.tui = TriageTUI(title="Test", github_repository="test/repo")
        self.tui.set_review_mode(True)

    def test_passing_ready(self):
        entry = _make_entry(category=PRCategory.PASSING)
        assert "ready to review" in self.tui.get_suggested_action(entry)

    def test_passing_llm_in_progress(self):
        entry = _make_entry(category=PRCategory.PASSING)
        entry.llm_status = "in_progress"
        assert "LLM reviewing" in self.tui.get_suggested_action(entry)

    def test_passing_llm_flagged(self):
        entry = _make_entry(category=PRCategory.PASSING)
        entry.llm_status = "flagged"
        assert "review findings" in self.tui.get_suggested_action(entry)

    def test_flagged_conflicts(self):
        entry = _make_entry(category=PRCategory.NON_LLM_ISSUES)
        entry.pr.mergeable = "CONFLICTING"
        assert "needs rebase" in self.tui.get_suggested_action(entry)

    def test_skipped(self):
        entry = _make_entry(category=PRCategory.PASSING)
        entry.action_taken = "skipped"
        assert "re-evaluate" in self.tui.get_suggested_action(entry)


# ===========================================================================
# TriageTUI — mouse handling
# ===========================================================================
class TestMouseHandling:
    def _make_tui_with_layout(self, n_entries: int = 10) -> TriageTUI:
        entries = [_make_entry(i) for i in range(n_entries)]
        tui = _make_tui_with_entries(entries)
        # Simulate layout geometry from a render
        tui._layout_header_end = 6
        tui._layout_list_start = 6
        tui._layout_list_end = 20
        tui._layout_bottom_start = 20
        tui._layout_bottom_end = 35
        tui._visible_rows = 12
        tui._diff_visible_lines = 10
        tui._detail_visible_lines = 10
        return tui

    def test_click_in_pr_list(self):
        tui = self._make_tui_with_layout()
        # Click on row at y=10 → data_row = 10 - 6 - 2 = 2, target_index = 0 + 2 = 2
        result = tui._handle_mouse(MouseEvent(x=5, y=10, button="click"))
        assert result is not None
        assert tui.cursor == 2

    def test_scroll_up_in_pr_list(self):
        tui = self._make_tui_with_layout()
        tui.cursor = 5
        result = tui._handle_mouse(MouseEvent(x=5, y=10, button="scroll_up"))
        assert result is not None
        assert tui.cursor < 5

    def test_scroll_down_in_pr_list(self):
        tui = self._make_tui_with_layout()
        result = tui._handle_mouse(MouseEvent(x=5, y=10, button="scroll_down"))
        assert result is not None
        assert tui.cursor > 0

    def test_click_in_detail_panel(self):
        tui = self._make_tui_with_layout()
        # Click in bottom-left (detail panel), assuming width > 2*x
        result = tui._handle_mouse(MouseEvent(x=5, y=25, button="click"))
        assert result is not None
        from airflow_breeze.utils.tui_display import _FocusPanel

        assert tui._focus == _FocusPanel.DETAIL

    def test_scroll_in_diff_panel(self):
        tui = self._make_tui_with_layout()
        tui.set_diff(1, "\n".join(f"line{i}" for i in range(100)))
        # Scroll in bottom-right (diff panel) — use x > half terminal width
        # _get_terminal_size returns 120 by default, so x=80 is right half
        result = tui._handle_mouse(MouseEvent(x=80, y=25, button="scroll_down"))
        assert result is not None
        assert tui._diff_scroll > 0

    def test_mouse_outside_panels_ignored(self):
        tui = self._make_tui_with_layout()
        # Click in footer area (below bottom_end)
        result = tui._handle_mouse(MouseEvent(x=5, y=36, button="click"))
        assert result is None


# ===========================================================================
# TriageTUI — mouse enable/disable
# ===========================================================================
class TestMouseToggle:
    def test_enable_disable_idempotent(self):
        tui = TriageTUI(title="Test")
        # Calling disable when not enabled should not error
        tui.disable_mouse()
        assert tui._mouse_enabled is False
        # Enable, then disable
        # (actual terminal escapes only work with TTY, but the flag should toggle)
        tui._mouse_enabled = True
        tui.disable_mouse()
        assert tui._mouse_enabled is False
