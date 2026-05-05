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

import importlib.util
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

# The script under test declares its runtime deps via PEP 723 inline metadata
# and is executed with ``uv run`` in production. The unit tests never construct
# the real HTTP client (they mock it), so we stub ``httpx`` in ``sys.modules``
# before loading the module. This keeps the scripts-project test venv slim —
# no need to add ``httpx`` to ``scripts/pyproject.toml`` just for tests.
sys.modules.setdefault("httpx", types.ModuleType("httpx"))

MODULE_PATH = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "notify_uv_lock_conflicts.py"


@pytest.fixture
def mod():
    module_name = "test_notify_uv_lock_conflicts_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Register in sys.modules *before* exec_module so @dataclass (under
    # ``from __future__ import annotations``) can resolve the defining module
    # via sys.modules[cls.__module__] when parsing string annotations.
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        yield module
    finally:
        sys.modules.pop(module_name, None)


# A recent timestamp — well inside the stale window — used by fixture PRs.
FRESH_ISO = "2099-01-01T12:00:00Z"
STALE_CUT = datetime(1970, 1, 1, tzinfo=timezone.utc)
SHORT_SHA = "abc1234"


def make_pr(
    *,
    number: int = 1,
    is_draft: bool = False,
    updated_at: str = FRESH_ISO,
    mergeable: str = "CONFLICTING",
    files: list[str] | None = None,
    comments: list[dict[str, Any]] | None = None,
    node_id: str = "PR_NODE",
) -> dict[str, Any]:
    return {
        "id": node_id,
        "number": number,
        "isDraft": is_draft,
        "updatedAt": updated_at,
        "mergeable": mergeable,
        "files": {
            "nodes": [{"path": p} for p in (files if files is not None else ["uv.lock"])],
            "totalCount": len(files) if files is not None else 1,
        },
        "comments": {"nodes": comments if comments is not None else []},
    }


class TestPrNumberRegex:
    @pytest.mark.parametrize(
        "headline,expected",
        [
            ("Fix bug in scheduler (#12345)", "12345"),
            ("Refactor deps   (#9) ", "9"),  # trailing whitespace
            ("Short (#1)\n", "1"),  # trailing newline
        ],
    )
    def test_matches_trailing_pr_number(self, mod, headline, expected):
        m = mod.PR_NUMBER_IN_COMMIT_RE.search(headline)
        assert m is not None and m.group(1) == expected

    @pytest.mark.parametrize(
        "headline",
        [
            "Fix bug (see #12345)",  # not at end
            "No PR ref",
            "(#notanumber)",
            "Reference (#1) somewhere in middle",
        ],
    )
    def test_does_not_match_non_trailing(self, mod, headline):
        assert mod.PR_NUMBER_IN_COMMIT_RE.search(headline) is None


class TestParseUpdatedAt:
    def test_handles_zulu_suffix(self, mod):
        dt = mod.parse_updated_at("2025-04-24T10:20:30Z")
        assert dt == datetime(2025, 4, 24, 10, 20, 30, tzinfo=timezone.utc)

    def test_handles_explicit_offset(self, mod):
        dt = mod.parse_updated_at("2025-04-24T10:20:30+00:00")
        assert dt == datetime(2025, 4, 24, 10, 20, 30, tzinfo=timezone.utc)


class TestClassify:
    def test_draft(self, mod):
        kind, entry = mod.classify(make_pr(is_draft=True), STALE_CUT, SHORT_SHA)
        assert kind == "drafts" and entry is None

    def test_stale(self, mod):
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=14)
        old_pr = make_pr(updated_at="2000-01-01T00:00:00Z")
        kind, entry = mod.classify(old_pr, cutoff, SHORT_SHA)
        assert kind == "stale" and entry is None

    def test_no_uv_lock(self, mod):
        kind, entry = mod.classify(make_pr(files=["other.txt"]), STALE_CUT, SHORT_SHA)
        assert kind == "no_uv_lock" and entry is None

    def test_already_notified_for_this_sha(self, mod):
        comments = [{"id": "C1", "body": f"{mod.MARKER}\nmessage referencing {SHORT_SHA}"}]
        kind, entry = mod.classify(make_pr(comments=comments), STALE_CUT, SHORT_SHA)
        assert kind == "already_notified" and entry is None

    def test_conflicting(self, mod):
        kind, entry = mod.classify(make_pr(mergeable="CONFLICTING"), STALE_CUT, SHORT_SHA)
        assert kind == "conflicting" and entry is not None and entry["existing"] is None

    def test_mergeable(self, mod):
        kind, entry = mod.classify(make_pr(mergeable="MERGEABLE"), STALE_CUT, SHORT_SHA)
        assert kind == "mergeable" and entry is not None

    def test_unknown(self, mod):
        kind, entry = mod.classify(make_pr(mergeable="UNKNOWN"), STALE_CUT, SHORT_SHA)
        assert kind == "unknown" and entry is not None

    def test_previous_marker_with_different_sha_still_notifies(self, mod):
        """An existing marker comment referencing an *older* sha must not short-circuit."""
        comments = [{"id": "C1", "body": f"{mod.MARKER}\nolder notice for deadbee"}]
        kind, entry = mod.classify(make_pr(comments=comments), STALE_CUT, SHORT_SHA)
        assert kind == "conflicting"
        assert entry is not None
        assert entry["existing"] == {"id": "C1", "body": f"{mod.MARKER}\nolder notice for deadbee"}


class TestBuildBody:
    def test_includes_marker_source_and_instructions(self, mod):
        body = mod.build_body("[#42](https://example/pr/42)")
        assert body.startswith(mod.MARKER)
        assert "[#42](https://example/pr/42)" in body
        assert "git fetch upstream main" in body
        assert "rm uv.lock && uv lock" in body
        assert "Automated nudge" in body


class TestResolveSourcePr:
    def test_extracts_pr_number_from_headline(self, mod):
        client = MagicMock()
        client.call.side_effect = [
            {"repository": {"object": {"messageHeadline": "Fix something (#987)"}}},
            {"repository": {"pullRequest": {"number": 987, "url": "U", "title": "T"}}},
        ]
        pr = mod.resolve_source_pr(client, "o", "r", "deadbeef", "deadbee")
        assert pr == {"number": 987, "url": "U", "title": "T"}
        assert client.call.call_count == 2

    def test_returns_none_when_no_pr_number(self, mod):
        client = MagicMock()
        client.call.return_value = {"repository": {"object": {"messageHeadline": "Direct push"}}}
        assert mod.resolve_source_pr(client, "o", "r", "deadbeef", "deadbee") is None
        assert client.call.call_count == 1  # second query skipped

    def test_returns_none_on_error(self, mod):
        client = MagicMock()
        client.call.side_effect = RuntimeError("boom")
        assert mod.resolve_source_pr(client, "o", "r", "deadbeef", "deadbee") is None


class TestScanOpenPrs:
    def _page(self, nodes, *, has_next=False, cursor=None):
        return {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                    "nodes": nodes,
                }
            }
        }

    def test_early_exit_on_stale(self, mod):
        """A stale PR stops pagination even if hasNextPage is True."""
        client = MagicMock()
        recent_cut = datetime.now(tz=timezone.utc) - timedelta(days=1)
        fresh = (datetime.now(tz=timezone.utc)).isoformat().replace("+00:00", "Z")
        stale = (datetime.now(tz=timezone.utc) - timedelta(days=30)).isoformat().replace("+00:00", "Z")
        client.call.return_value = self._page(
            [
                make_pr(number=1, updated_at=fresh, mergeable="CONFLICTING"),
                make_pr(number=2, updated_at=stale),  # triggers early exit
            ],
            has_next=True,
            cursor="NEXT",
        )
        stats = mod.Stats()
        confirmed, unknowns = mod.scan_open_prs(client, "o", "r", recent_cut, SHORT_SHA, stats)
        assert len(confirmed) == 1 and confirmed[0]["pr"]["number"] == 1
        assert unknowns == []
        assert stats.stale == 1 and stats.conflicting == 1
        assert client.call.call_count == 1  # did not fetch page 2

    def test_buckets_prs_correctly(self, mod):
        client = MagicMock()
        client.call.return_value = self._page(
            [
                make_pr(number=1, mergeable="CONFLICTING"),
                make_pr(number=2, mergeable="UNKNOWN"),
                make_pr(number=3, mergeable="MERGEABLE"),
                make_pr(number=4, is_draft=True),
                make_pr(number=5, files=["other.txt"]),
            ]
        )
        stats = mod.Stats()
        confirmed, unknowns = mod.scan_open_prs(client, "o", "r", STALE_CUT, SHORT_SHA, stats)
        assert [c["pr"]["number"] for c in confirmed] == [1]
        assert [u["pr"]["number"] for u in unknowns] == [2]
        assert stats.conflicting == 1
        assert stats.unknown == 1
        assert stats.mergeable == 1
        assert stats.drafts == 1
        assert stats.no_uv_lock == 1

    def test_paginates_until_exhausted(self, mod):
        client = MagicMock()
        client.call.side_effect = [
            self._page([make_pr(number=1)], has_next=True, cursor="C1"),
            self._page([make_pr(number=2)], has_next=False),
        ]
        stats = mod.Stats()
        confirmed, _ = mod.scan_open_prs(client, "o", "r", STALE_CUT, SHORT_SHA, stats)
        assert [c["pr"]["number"] for c in confirmed] == [1, 2]
        assert stats.pages == 2


class TestRetryUnknowns:
    @pytest.fixture(autouse=True)
    def _patch_sleep(self, mod, monkeypatch):
        """Skip real sleeps so the retry loop is instantaneous under test."""
        monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    def test_unknowns_resolve_on_retry(self, mod):
        client = MagicMock()
        initial_unknown = {"pr": make_pr(number=7, mergeable="UNKNOWN"), "existing": None}
        client.call.return_value = {"repository": {"pullRequest": make_pr(number=7, mergeable="CONFLICTING")}}
        stats = mod.Stats()
        confirmed: list[dict[str, Any]] = []
        remaining = mod.retry_unknowns(
            client, "o", "r", STALE_CUT, SHORT_SHA, confirmed, [initial_unknown], stats
        )
        assert remaining == []
        assert [c["pr"]["number"] for c in confirmed] == [7]
        assert stats.retries == 1

    def test_gives_up_after_max_retries(self, mod):
        client = MagicMock()
        # Always returns UNKNOWN — simulate a PR GitHub never resolves during the run.
        client.call.return_value = {"repository": {"pullRequest": make_pr(number=8, mergeable="UNKNOWN")}}
        stats = mod.Stats()
        initial = [{"pr": make_pr(number=8, mergeable="UNKNOWN"), "existing": None}]
        remaining = mod.retry_unknowns(client, "o", "r", STALE_CUT, SHORT_SHA, [], initial, stats)
        assert [r["pr"]["number"] for r in remaining] == [8]
        assert stats.retries == mod.MAX_RETRIES
        assert client.call.call_count == mod.MAX_RETRIES


class TestPostNotices:
    def test_updates_when_marker_exists_and_creates_otherwise(self, mod):
        client = MagicMock()
        existing_pr = make_pr(number=1, node_id="PR_A")
        new_pr = make_pr(number=2, node_id="PR_B")
        confirmed = [
            {"pr": existing_pr, "existing": {"id": "C_OLD", "body": f"{mod.MARKER}\nold"}},
            {"pr": new_pr, "existing": None},
        ]
        stats = mod.Stats()
        mod.post_notices(client, confirmed, "BODY", stats)
        assert stats.updated == 1 and stats.posted == 1
        assert client.call.call_count == 2
        # First call must be the update mutation (existing marker); second must be create.
        (update_query, update_vars), _ = client.call.call_args_list[0]
        (create_query, create_vars), _ = client.call.call_args_list[1]
        assert "updateIssueComment" in update_query
        assert update_vars == {"id": "C_OLD", "body": "BODY"}
        assert "addComment" in create_query
        assert create_vars == {"subjectId": "PR_B", "body": "BODY"}


class TestWriteSummary:
    def test_noop_without_path(self, mod):
        # Should silently do nothing; absence of an error is the assertion.
        mod.write_summary(None, SHORT_SHA, "ref", mod.Stats())

    def test_writes_table_when_path_given(self, mod, tmp_path):
        path = tmp_path / "summary.md"
        stats = mod.Stats(scanned=5, pages=1, conflicting=2, unknown=1, mergeable=1, posted=2, updated=0)
        mod.write_summary(str(path), SHORT_SHA, "**PR ref**", stats)
        text = path.read_text()
        assert f"uv.lock conflict notifier — {SHORT_SHA}" in text
        assert "**Source of change:** **PR ref**" in text
        assert "| Scanned | 5 |" in text
        assert "| Conflicting | 2 |" in text
        assert "| Notices posted | 2 |" in text
