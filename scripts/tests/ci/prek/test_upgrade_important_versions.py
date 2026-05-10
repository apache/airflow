#
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
"""Unit tests for ``scripts/ci/prek/upgrade_important_versions.py``."""

from __future__ import annotations

from unittest import mock

import pytest


@pytest.fixture
def fake_dockerhub_response(monkeypatch):
    """Patch `requests.get` used by `get_latest_image_version` to return a fixed tag list."""

    def _install(tag_names: list[str]) -> mock.MagicMock:
        from ci.prek import upgrade_important_versions as uiv

        response = mock.MagicMock()
        response.raise_for_status = mock.MagicMock()
        response.json = mock.MagicMock(return_value={"results": [{"name": n} for n in tag_names]})
        get = mock.MagicMock(return_value=response)
        monkeypatch.setattr(uiv.requests, "get", get)
        return get

    return _install


@pytest.mark.parametrize(
    ("tags", "expected"),
    [
        pytest.param(
            ["20260127", "3.23.5", "3.23", "3.22.0", "3", "latest", "edge"],
            "3.23.5",
            id="alpine-real-tag-mix-rejects-date-and-edge",
        ),
        pytest.param(
            ["1.37.0", "1.37", "1", "1.36.1", "musl", "stable"],
            "1.37.0",
            id="busybox-style-tags",
        ),
        pytest.param(
            ["v20260127", "v3.23.5"],
            "v3.23.5",
            id="v-prefixed-tags-with-date",
        ),
        pytest.param(
            ["20260127.1", "3.23.5"],
            "3.23.5",
            id="date-with-revision-suffix-still-rejected",
        ),
    ],
)
def test_get_latest_image_version_rejects_date_shaped_tags(fake_dockerhub_response, tags, expected):
    """A date-shaped daily-build tag must not be picked over a release tag.

    Regression for the alpine `20260127` bump in apache/airflow#66580 — the
    bumper used `packaging.version.Version` to sort tags, but PEP 440 happily
    parses `20260127` as a single-component version that sorts above `3.23`,
    so the script auto-pinned the daily edge image instead of the latest
    release. The fix filters date-shaped tags before the version sort.
    """
    fake_dockerhub_response(tags)
    from ci.prek.upgrade_important_versions import get_latest_image_version

    assert get_latest_image_version("alpine") == expected


def test_date_shaped_tag_regex_matches_only_date_stamps():
    """The pre-filter regex matches date-stamped tags, not legitimate releases."""
    from ci.prek.upgrade_important_versions import _DATE_SHAPED_TAG_RE

    # Date-shaped tags — should match (and therefore be skipped).
    for tag in ["20260127", "v20260127", "20260127.1", "v20260127.10"]:
        assert _DATE_SHAPED_TAG_RE.match(tag) is not None, f"expected match for {tag!r}"

    # Release tags — must not match.
    for tag in ["3.23", "3.23.5", "1.37.0", "v1.37.0", "1", "3", "latest", "stable"]:
        assert _DATE_SHAPED_TAG_RE.match(tag) is None, f"unexpected match for {tag!r}"
