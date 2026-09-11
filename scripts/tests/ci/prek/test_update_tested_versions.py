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

import textwrap

from ci.prek import update_tested_versions as hook
from ci.prek.common_prek_utils import (
    read_allowed_python_major_minor_versions,
    read_current_postgres_versions,
)


def _dev_cell(line: str) -> str:
    """The (stripped) "Main version (dev)" cell of a markdown table row."""
    return line.split("|")[1:-1][1].strip()


def _stable_cells(line: str) -> list[str]:
    """The stable-column cells (everything after the dev column)."""
    return [c.strip() for c in line.split("|")[1:-1][2:]]


class TestRenderPrerequisitesBlock:
    def test_block_reflects_constants(self):
        block = hook.render_prerequisites_block()
        assert f"* Python: {', '.join(read_allowed_python_major_minor_versions())}\n" in block
        assert f"  * PostgreSQL: {', '.join(read_current_postgres_versions())}\n" in block
        # SQLite has no central constant - it is rendered from the hook's own value.
        assert f"  * SQLite: {hook.SQLITE_VERSION}\n" in block
        # MySQL always carries the editorial "Innovation" annotation.
        assert "Innovation" in block

    def test_block_is_wrapped_by_markers_content_only(self):
        # The rendered block must not itself contain the markers (those wrap it).
        block = hook.render_prerequisites_block()
        assert hook.PREREQUISITES_START not in block
        assert hook.PREREQUISITES_END not in block


class TestUpdateReadme:
    TABLE = textwrap.dedent(
        """\
        ## Requirements

        |            | Main version (dev) | Stable version (3.2.0) | Stable version (2.11.2) |
        |------------|--------------------|------------------------|-------------------------|
        | Python     | WRONG              | 3.10, 3.11             | 3.10                    |
        | Platform   | AMD64/ARM64        | AMD64/ARM64            | AMD64/ARM64             |
        | PostgreSQL | WRONG              | 14, 15                 | 12, 13                  |
        | SQLite     | 3.15.0+            | 3.15.0+                | 3.15.0+                 |

        text after the table
        """
    )

    def test_syncs_dev_column_and_preserves_stable_columns(self, tmp_path, monkeypatch):
        readme = tmp_path / "README.md"
        readme.write_text(self.TABLE)
        monkeypatch.setattr(hook, "README_MD", readme)

        assert hook.update_readme() is True

        lines = readme.read_text().splitlines()
        rows = {line.split("|")[1].strip(): line for line in lines if line.startswith("|")}

        expected_python = ", ".join(read_allowed_python_major_minor_versions())
        expected_postgres = ", ".join(read_current_postgres_versions())

        # Dev column is now synced from the constants.
        assert _dev_cell(rows["Python"]) == expected_python
        assert _dev_cell(rows["PostgreSQL"]) == expected_postgres

        # Stable columns are untouched.
        assert _stable_cells(rows["Python"]) == ["3.10, 3.11", "3.10"]
        assert _stable_cells(rows["PostgreSQL"]) == ["14, 15", "12, 13"]

        # A non-target row keeps its dev value verbatim.
        assert _dev_cell(rows["Platform"]) == "AMD64/ARM64"

        # Text outside the table is preserved.
        assert "text after the table" in readme.read_text()

    def test_noop_when_already_synced(self, tmp_path, monkeypatch):
        readme = tmp_path / "README.md"
        readme.write_text(self.TABLE)
        monkeypatch.setattr(hook, "README_MD", readme)

        # First pass syncs and reports a change; a second pass is a no-op.
        assert hook.update_readme() is True
        synced = readme.read_text()
        assert hook.update_readme() is False
        assert readme.read_text() == synced


class TestRepoDocsInSync:
    """Regression guard: the committed docs must already match the constants."""

    def test_prerequisites_in_sync(self):
        assert hook.update_prerequisites() is False, (
            "prerequisites.rst is out of sync with global_constants.py - "
            "run 'prek run update-tested-versions --all-files'"
        )

    def test_readme_in_sync(self):
        assert hook.update_readme() is False, (
            "README.md tested-versions table is out of sync with global_constants.py - "
            "run 'prek run update-tested-versions --all-files'"
        )
