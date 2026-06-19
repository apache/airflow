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

from pathlib import Path

import pytest
from check_http_exception_import_from_fastapi import check_file


class TestCheckFile:
    @pytest.mark.parametrize(
        "code, expected",
        [
            pytest.param(
                "from starlette.exceptions import HTTPException\n",
                [(1, "from starlette.exceptions import HTTPException")],
                id="starlette-exceptions",
            ),
            pytest.param(
                "from http.client import HTTPException\n",
                [(1, "from http.client import HTTPException")],
                id="http-client",
            ),
            pytest.param(
                "from starlette.exceptions import HTTPException as StarletteHTTPException\n",
                [(1, "from starlette.exceptions import HTTPException as StarletteHTTPException")],
                id="aliased-starlette",
            ),
            pytest.param(
                "from http.client import HTTPException, HTTPSConnection\n",
                [(1, "from http.client import HTTPException")],
                id="mixed-import-with-extra-names",
            ),
            pytest.param(
                "from fastapi import HTTPException\n"
                "from starlette.exceptions import HTTPException as StarletteHTTPException\n",
                [(2, "from starlette.exceptions import HTTPException as StarletteHTTPException")],
                id="good-and-bad-mixed",
            ),
        ],
    )
    def test_violations_detected(self, write_python_file, code: str, expected: list[tuple[int, str]]):
        f = write_python_file(code)
        assert check_file(f) == expected

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param("from fastapi import HTTPException\n", id="from-fastapi"),
            pytest.param(
                "from fastapi.exceptions import HTTPException\n",
                id="from-fastapi-exceptions",
            ),
            pytest.param(
                "from fastapi import Depends, HTTPException, status\n",
                id="multi-name-from-fastapi",
            ),
            pytest.param(
                "from fastapi import HTTPException as HTTPExc\n",
                id="aliased-from-fastapi",
            ),
            pytest.param("import fastapi\n", id="import-fastapi-module"),
            pytest.param("from http.client import HTTPSConnection\n", id="unrelated-import"),
            pytest.param("x = 1\n", id="no-imports"),
        ],
    )
    def test_no_violation(self, write_python_file, code: str):
        f = write_python_file(code)
        assert check_file(f) == []

    def test_syntax_error_is_silently_skipped(self, write_python_file):
        f = write_python_file("def broken(:\n")
        assert check_file(f) == []

    def test_missing_file_is_silently_skipped(self, tmp_path: Path):
        assert check_file(tmp_path / "does_not_exist.py") == []
