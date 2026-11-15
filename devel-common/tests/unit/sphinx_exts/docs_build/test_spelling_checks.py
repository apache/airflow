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

import json

import pytest

from sphinx_exts.docs_build.code_utils import AIRFLOW_CONTENT_ROOT_PATH
from sphinx_exts.docs_build.spelling_checks import (
    SpellingError,
    emit_github_annotations,
    load_spelling_errors,
)

SOURCE = AIRFLOW_CONTENT_ROOT_PATH / "task-sdk" / "src" / "airflow" / "sdk" / "importers" / "base.py"


def test_load_spelling_errors(tmp_path):
    output = tmp_path / "output-spelling.json"
    output.write_text(
        json.dumps(
            [
                {"file": SOURCE.as_posix(), "line": 86, "word": "routable", "context": "source routable"},
                {"file": None, "line": None, "word": "zorp", "context": "a zorp"},
            ]
        )
    )

    errors = load_spelling_errors(output)

    assert errors == [
        SpellingError(
            file_path=SOURCE,
            line_no=86,
            spelling="routable",
            suggestion=None,
            context_line="source routable",
            message="task-sdk/src/airflow/sdk/importers/base.py:86: (routable) source routable",
        ),
        SpellingError(
            file_path=None,
            line_no=None,
            spelling="zorp",
            suggestion=None,
            context_line="a zorp",
            message="<unknown>:None: (zorp) a zorp",
        ),
    ]


class TestEmitGithubAnnotations:
    ERRORS = {
        "task-sdk": [
            SpellingError(
                file_path=SOURCE,
                line_no=86,
                spelling="routable",
                suggestion=None,
                context_line="50% done, really: yes",
                message="",
            ),
            SpellingError(
                file_path=SOURCE.with_name("base.py:docstring of airflow.sdk.FileDagDefinition"),
                line_no=4,
                spelling="routable",
                suggestion=None,
                context_line="source routable",
                message="",
            ),
            SpellingError(
                file_path=None,
                line_no=None,
                spelling=None,
                suggestion=None,
                context_line=None,
                message="Spelling was not checked",
            ),
        ]
    }

    def test_prints_nothing_for_pages_generated_by_autoapi(self, monkeypatch, capsys, tmp_path):
        generated_page = tmp_path / "docs" / "_api" / "airflow" / "sdk" / "index.rst"
        generated_page.parent.mkdir(parents=True)
        generated_page.write_text("generated")
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        error = SpellingError(
            file_path=generated_page,
            line_no=12,
            spelling="routable",
            suggestion=None,
            context_line="source routable",
            message="",
        )

        emit_github_annotations({"task-sdk": [error]})

        assert capsys.readouterr().out == ""

    def test_prints_one_error_annotation_per_misspelling_in_a_real_file(self, monkeypatch, capsys):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")

        emit_github_annotations(self.ERRORS)

        assert capsys.readouterr().out == (
            "::error file=task-sdk/src/airflow/sdk/importers/base.py,line=86,title=Spelling::"
            "Unknown word 'routable' in: 50%25 done, really: yes. If it is spelled correctly, "
            "quote code in backticks or add the word to docs/spelling_wordlist.txt.\n"
        )

    @pytest.mark.parametrize("github_actions", [None, "false"])
    def test_prints_nothing_outside_github_actions(self, monkeypatch, capsys, github_actions):
        if github_actions is None:
            monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        else:
            monkeypatch.setenv("GITHUB_ACTIONS", github_actions)

        emit_github_annotations(self.ERRORS)

        assert capsys.readouterr().out == ""
