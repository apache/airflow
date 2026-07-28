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

from ci.prek.sync_uv_min_version_markers import MARKER_RE, sync_file


class TestSyncFile:
    def test_updates_marked_version(self, tmp_path):
        path = tmp_path / "fixture.py"
        path.write_text(
            '_MIN_UV = "0.9.17"  # sync-uv-min-version\n'
            '_other = "0.9.17"\n'
            'stdout = f"uv {_MIN_UV} (fake)"  # no marker — unchanged\n'
        )
        assert sync_file(path, "0.12.0") is True
        contents = path.read_text()
        assert '_MIN_UV = "0.12.0"  # sync-uv-min-version\n' in contents
        # Non-marked literal stays untouched.
        assert '_other = "0.9.17"\n' in contents

    def test_noop_when_already_synced(self, tmp_path):
        path = tmp_path / "fixture.py"
        original = '_MIN_UV = "0.12.0"  # sync-uv-min-version\n'
        path.write_text(original)
        assert sync_file(path, "0.12.0") is False
        assert path.read_text() == original

    def test_handles_multiple_markers(self, tmp_path):
        path = tmp_path / "fixture.py"
        path.write_text(
            '_FLOOR = "0.9.17"  # sync-uv-min-version\n'
            '_MATCHING = "0.9.17"  # sync-uv-min-version (matching actual uv)\n'
        )
        assert sync_file(path, "0.12.0") is True
        contents = path.read_text()
        assert contents.count('"0.12.0"  # sync-uv-min-version') == 2

    def test_supports_single_quotes(self, tmp_path):
        path = tmp_path / "fixture.py"
        path.write_text("_MIN_UV = '0.9.17'  # sync-uv-min-version\n")
        assert sync_file(path, "0.12.0") is True
        assert "_MIN_UV = '0.12.0'  # sync-uv-min-version\n" in path.read_text()


class TestMarkerRegex:
    def test_matches_marker_with_double_quotes(self):
        match = MARKER_RE.search('x = "0.9.17"  # sync-uv-min-version')
        assert match is not None
        assert match.group("version") == "0.9.17"

    def test_does_not_match_unmarked_version(self):
        assert MARKER_RE.search('x = "0.9.17"') is None

    def test_does_not_match_other_markers(self):
        assert MARKER_RE.search('x = "0.9.17"  # Keep this comment to allow ...') is None
