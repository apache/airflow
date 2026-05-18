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

import contextlib
import sys
from pathlib import Path
from unittest import mock
from urllib.error import URLError

import pytest

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]

AIRFLOW_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(AIRFLOW_ROOT / "scripts" / "ci"))
import fetch_theme_assets  # noqa: E402


class TestSphinxThemeWorkspace:
    def test_theme_is_workspace_member(self):
        with open(AIRFLOW_ROOT / "pyproject.toml", "rb") as f:
            root_config = tomllib.load(f)
        members = root_config["tool"]["uv"]["workspace"]["members"]
        assert "docs-theme" in members

    def test_theme_source_exists(self):
        theme_dir = AIRFLOW_ROOT / "docs-theme" / "sphinx_airflow_theme"
        assert theme_dir.is_dir()
        assert (theme_dir / "__init__.py").is_file()
        assert (theme_dir / "theme.conf").is_file()
        assert (theme_dir / "layout.html").is_file()

    def test_theme_has_valid_pyproject(self):
        with open(AIRFLOW_ROOT / "docs-theme" / "pyproject.toml", "rb") as f:
            config = tomllib.load(f)
        assert config["project"]["name"] == "sphinx_airflow_theme"
        assert config["build-system"]["build-backend"] == "flit_core.buildapi"
        entry_points = config["project"]["entry-points"]["sphinx.html_themes"]
        assert "sphinx_airflow_theme" in entry_points

    def test_devel_common_uses_workspace_dep(self):
        with open(AIRFLOW_ROOT / "devel-common" / "pyproject.toml", "rb") as f:
            config = tomllib.load(f)
        docs_deps = config["project"]["optional-dependencies"]["docs"]
        theme_deps = [d for d in docs_deps if "sphinx-airflow-theme" in d.lower()]
        assert len(theme_deps) == 1, f"Expected exactly one theme dep, got: {theme_deps}"
        assert "@https://" not in theme_deps[0], (
            f"Theme should be a workspace dep, not a URL: {theme_deps[0]}"
        )

    def test_theme_registered_in_uv_sources(self):
        with open(AIRFLOW_ROOT / "pyproject.toml", "rb") as f:
            root_config = tomllib.load(f)
        sources = root_config["tool"]["uv"]["sources"]
        assert "sphinx-airflow-theme" in sources
        assert sources["sphinx-airflow-theme"] == {"workspace": True}

    def test_gen_dir_is_gitignored(self):
        gitignore = (AIRFLOW_ROOT / "docs-theme" / ".gitignore").read_text()
        assert "static/_gen/" in gitignore

    def test_fetch_script_exists(self):
        fetch_script = AIRFLOW_ROOT / "scripts" / "ci" / "fetch_theme_assets.py"
        assert fetch_script.is_file()


class TestFetchThemeRetry:
    @mock.patch("fetch_theme_assets.time.sleep")
    @mock.patch("fetch_theme_assets.urlopen")
    def test_retries_on_transient_failure(self, mock_urlopen, mock_sleep, tmp_path):
        mock_urlopen.side_effect = [
            URLError("Connection timed out"),
            URLError("Connection reset"),
            mock.MagicMock(read=lambda: (tmp_path / "dummy.whl").write_bytes(b"") or b"PK\x03\x04"),
        ]
        with mock.patch.object(fetch_theme_assets, "WHEEL_URL", "https://example.com/{version}.whl"):
            with mock.patch.object(fetch_theme_assets, "THEME_DIR", tmp_path / "theme"):
                with mock.patch.object(fetch_theme_assets, "GEN_DIR", tmp_path / "gen"):
                    with mock.patch.object(
                        fetch_theme_assets, "VERSION_STAMP", tmp_path / "gen" / ".version"
                    ):
                        # Will fail on zipfile parse, but we only care that retry logic reached attempt 3
                        with contextlib.suppress(Exception):
                            fetch_theme_assets.fetch_and_extract("0.0.1")
        assert mock_urlopen.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(2)
        mock_sleep.assert_any_call(4)

    @mock.patch("fetch_theme_assets.time.sleep")
    @mock.patch("fetch_theme_assets.urlopen")
    def test_exits_after_max_retries(self, mock_urlopen, mock_sleep):
        mock_urlopen.side_effect = URLError("Connection refused")
        with pytest.raises(SystemExit, match="1"):
            fetch_theme_assets.fetch_and_extract("0.0.1")
        assert mock_urlopen.call_count == 3
