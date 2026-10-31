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

import pytest
from check_rst_license_header import fix_file, get_canonical_header, main

CANONICAL = get_canonical_header()
BODY = ["Title", "=====", "", "Text"]


def write_rst(tmp_path, lines, newline="\n"):
    path = tmp_path / "doc.rst"
    path.write_bytes(newline.join(lines).encode())
    return path


class TestGetCanonicalHeader:
    def test_indents_every_non_empty_line_by_one_space(self):
        assert CANONICAL[0].startswith(" .. Licensed to the Apache")
        assert CANONICAL[1].startswith("    or more contributor")
        assert "" in CANONICAL
        assert all(line.startswith(" ") for line in CANONICAL if line)


class TestFixFile:
    def test_leaves_canonical_header_untouched(self, tmp_path):
        path = write_rst(tmp_path, [*CANONICAL, *BODY])

        assert fix_file(path, CANONICAL) is False
        assert path.read_text().splitlines() == [*CANONICAL, *BODY]

    @pytest.mark.parametrize(
        "header",
        [
            pytest.param([line.lstrip(" ") for line in CANONICAL], id="column_zero"),
            pytest.param(["", *CANONICAL], id="leading_blank_line"),
            pytest.param(["", "", *CANONICAL], id="leading_blank_lines"),
            pytest.param(["", *[line.lstrip(" ") for line in CANONICAL]], id="blank_line_and_column_zero"),
        ],
    )
    def test_rewrites_blank_line_or_indentation_deviations(self, tmp_path, header):
        path = write_rst(tmp_path, [*header, *BODY])

        assert fix_file(path, CANONICAL) is True
        assert path.read_text().splitlines() == [*CANONICAL, *BODY]
        assert fix_file(path, CANONICAL) is False

    def test_preserves_crlf_line_endings(self, tmp_path):
        path = write_rst(tmp_path, ["", *CANONICAL, *BODY], newline="\r\n")

        assert fix_file(path, CANONICAL) is True
        assert path.read_bytes() == "\r\n".join([*CANONICAL, *BODY]).encode()

    @pytest.mark.parametrize(
        "lines",
        [
            pytest.param([":tocdepth: 4", "", *CANONICAL, *BODY], id="content_before_header"),
            pytest.param(["Title", *CANONICAL, *BODY], id="line_before_header"),
            pytest.param(BODY, id="no_header"),
        ],
    )
    def test_rejects_header_that_is_not_at_the_top(self, tmp_path, lines):
        path = write_rst(tmp_path, lines)
        before = path.read_bytes()

        with pytest.raises(ValueError, match="at the top of the file"):
            fix_file(path, CANONICAL)
        assert path.read_bytes() == before


class TestMain:
    def test_exits_zero_when_all_files_are_canonical(self, tmp_path):
        path = write_rst(tmp_path, [*CANONICAL, *BODY])

        assert main([str(path)]) == 0

    def test_exits_non_zero_after_rewriting_a_file(self, tmp_path, capsys):
        path = write_rst(tmp_path, ["", *CANONICAL, *BODY])

        assert main([str(path)]) == 1
        assert f"Fixed the license header shape of {path}" in capsys.readouterr().out
        assert main([str(path)]) == 0

    def test_reports_file_with_content_before_header(self, tmp_path, capsys):
        path = write_rst(tmp_path, [":tocdepth: 4", "", *CANONICAL, *BODY])

        assert main([str(path)]) == 1
        assert f"{path}: the license header must be at the top" in capsys.readouterr().out

    def test_skips_files_that_must_keep_their_field_list_first(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        path = tmp_path / "airflow-core/docs/cli-and-env-variables-ref.rst"
        path.parent.mkdir(parents=True)
        path.write_text("\n".join([":tocdepth: 4", "", *CANONICAL, *BODY]))

        assert main(["airflow-core/docs/cli-and-env-variables-ref.rst"]) == 0
