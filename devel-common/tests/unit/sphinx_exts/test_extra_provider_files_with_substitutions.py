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
from types import SimpleNamespace
from unittest import mock

import pytest
from sphinx.builders.html import StandaloneHTMLBuilder

from sphinx_exts import extra_provider_files_with_substitutions as substitutions


@pytest.fixture
def app(tmp_path):
    return SimpleNamespace(
        builder=mock.create_autospec(StandaloneHTMLBuilder, instance=True),
        config=SimpleNamespace(version="9.8.7"),
        outdir=tmp_path,
    )


@pytest.mark.parametrize("heading", ["1.2.3 (2026-10-01)", "1.2.3 (YANKED)", "Changelog"])
@mock.patch.object(substitutions, "_get_all_versions", autospec=True)
def test_modern_changelog_does_not_fetch_release_dates(mock_versions, app, tmp_path, heading):
    changelog = tmp_path / "changelog.html"
    changelog.write_text(
        f'<h2>{heading}<a href="#version">link</a></h2>\n'
        "<p>Install |version|</p>\n"
        "<p>Release Date: |PypiReleaseDate|</p>\n"
    )
    guide = tmp_path / "guide.html"
    guide.write_text('<h2>1.2.3<a href="#example">example</a></h2>\n<p>|version|</p>\n')

    substitutions.fix_provider_references(app, None)

    mock_versions.assert_not_called()
    assert changelog.read_text() == (f'<h2>{heading}<a href="#version">link</a></h2>\n<p>Install 9.8.7</p>\n')
    assert guide.read_text() == '<h2>1.2.3<a href="#example">example</a></h2>\n<p>9.8.7</p>\n'


@mock.patch.object(
    substitutions, "_get_all_versions", autospec=True, side_effect=ConnectionError("PyPI unavailable")
)
def test_release_lookup_failure_preserves_all_original_html(mock_versions, app, tmp_path):
    guide = tmp_path / "guide.html"
    guide.write_text("<p>Install |version|</p>\n")
    changelog = tmp_path / "changelog.html"
    changelog.write_text("<h2>1.2.3<a>link</a></h2>\n")
    originals = {path: path.read_bytes() for path in tmp_path.rglob("*.html")}

    with pytest.raises(ConnectionError, match="PyPI unavailable"):
        substitutions.fix_provider_references(app, None)

    assert {path: path.read_bytes() for path in originals} == originals


@pytest.mark.parametrize("version", ["1.2.3", "1!2.3.4", "5.0"])
@mock.patch.object(substitutions, "_get_all_versions", autospec=True)
def test_legacy_changelog_retains_ordered_duplicate_release_dates(
    mock_versions, app, tmp_path, monkeypatch, version
):
    monkeypatch.setenv("AIRFLOW_PACKAGE_NAME", "apache-airflow-providers-example")
    mock_versions.return_value = [(version, "2026-09-30"), (version, "2026-10-01")]
    heading = f'<h2>{version}<a href="#version">link</a></h2>\n'
    changelog = tmp_path / "changelog.html"
    changelog.write_text(f"<h2>Modern (2026-10-02)<a>link</a></h2>\n{heading}{heading}<p>|version|</p>\n")

    substitutions.fix_provider_references(app, None)

    mock_versions.assert_called_once_with("apache-airflow-providers-example")
    date_prefix = '<p>Release Date: <code class="docutils literal notranslate"><span class="pre">'
    date_suffix = "</span></code></p>"
    assert changelog.read_text() == (
        "<h2>Modern (2026-10-02)<a>link</a></h2>\n"
        f"{heading}{date_prefix}2026-09-30{date_suffix}"
        f"{heading}{date_prefix}2026-10-01{date_suffix}<p>9.8.7</p>\n"
    )


@mock.patch.object(substitutions, "_get_all_versions", autospec=True)
def test_release_dates_are_scanned_only_for_release_headings(mock_versions, app, tmp_path):
    dates = [("1.2.3", "2026-10-01"), ("1.2.2", "2026-09-01")]
    versions = mock.create_autospec(list, instance=True)
    versions.__iter__.side_effect = lambda: iter(dates)
    versions.remove.side_effect = dates.remove
    mock_versions.return_value = versions
    paragraphs = "<p>Change description</p>\n" * 100
    changelog = tmp_path / "changelog.html"
    changelog.write_text(
        f"{paragraphs}<h2>1.2.3<a>link</a></h2>\n{paragraphs}"
        f"<h2>Unrelated heading<a>link</a></h2>\n{paragraphs}<h2>1.2.2<a>link</a></h2>\n"
    )

    substitutions.fix_provider_references(app, None)

    assert versions.__iter__.call_count <= 2
    rendered = changelog.read_text()
    assert rendered.count("Release Date:") == 2
    assert "2026-10-01" in rendered
    assert "2026-09-01" in rendered


@pytest.mark.parametrize("relative_path", ["_api/airflow/example/index.html", "guide.html"])
@mock.patch.object(substitutions, "_get_all_versions", autospec=True)
def test_unchanged_html_is_not_rewritten(mock_versions, app, tmp_path, relative_path):
    page = tmp_path / relative_path
    page.parent.mkdir(parents=True, exist_ok=True)
    original = "<html><main><p>Unchanged café</p></main></html>\n".encode()
    page.write_bytes(original)
    os.utime(page, ns=(1_000_000_000, 1_000_000_000))
    original_mtime = page.stat().st_mtime_ns

    substitutions.fix_provider_references(app, None)

    assert page.read_bytes() == original
    assert page.stat().st_mtime_ns == original_mtime


@pytest.mark.parametrize("newline", [b"\r\n", b"\r"])
@mock.patch.object(substitutions, "_get_all_versions", autospec=True)
def test_existing_newline_normalization_is_preserved(mock_versions, app, tmp_path, newline):
    page = tmp_path / "guide.html"
    page.write_bytes(newline.join([b"<p>First paragraph</p>", b"<p>Second paragraph</p>", b""]))

    substitutions.fix_provider_references(app, None)

    assert page.read_bytes() == b"<p>First paragraph</p>\n<p>Second paragraph</p>\n"
