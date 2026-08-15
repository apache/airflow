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
from check_openapi_exception_doc_in_sync import check_file


class TestCheckFile:
    @pytest.mark.parametrize(
        "code, expected",
        [
            pytest.param(
                """
                @router.get("/x")
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                [("handler", 404, 4)],
                id="no-responses-block-at-all",
            ),
            pytest.param(
                """
                @router.get(
                    "/x",
                    responses=create_openapi_http_exception_doc([status.HTTP_409_CONFLICT]),
                )
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                [("handler", 404, 7)],
                id="status-missing-from-responses",
            ),
            pytest.param(
                """
                @router.post("/x")
                def handler():
                    if a:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="bad")
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="taken")
                """,
                [("handler", 400, 5), ("handler", 409, 6)],
                id="several-undeclared-statuses-are-all-reported",
            ),
            pytest.param(
                """
                @router.get("/x")
                def handler():
                    raise HTTPException(HTTP_404_NOT_FOUND, "nope")
                """,
                [("handler", 404, 4)],
                id="bare-status-constant",
            ),
            pytest.param(
                """
                @router.get("/x")
                def handler():
                    raise HTTPException(404, "nope")
                """,
                [("handler", 404, 4)],
                id="literal-status-code",
            ),
            pytest.param(
                """
                @router.get("/x")
                async def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                [("handler", 404, 4)],
                id="async-handler",
            ),
            pytest.param(
                """
                @router.get("/x")
                def handler():
                    def fail():
                        raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                    fail()
                """,
                [("handler", 404, 5)],
                id="raise-nested-inside-handler",
            ),
            pytest.param(
                """
                @router.delete(
                    "/x",
                    responses=create_openapi_http_exception_doc(
                        [(status.HTTP_409_CONFLICT, "conflict")]
                    ),
                )
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                [("handler", 404, 9)],
                id="tuple-form-declares-a-different-status",
            ),
            pytest.param(
                """
                router = APIRouter(
                    responses={status.HTTP_409_CONFLICT: {"description": "conflict"}},
                )

                @router.get("/x")
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                [("handler", 404, 8)],
                id="router-declares-a-different-status",
            ),
            pytest.param(
                """
                @router.get(
                    "/x",
                    responses={
                        status.HTTP_200_OK: {"description": "ok"},
                    },
                )
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                [("handler", 404, 9)],
                id="mapping-responses-without-the-status",
            ),
        ],
    )
    def test_violations_detected(self, write_python_file, code: str, expected):
        assert check_file(write_python_file(code)) == expected

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param(
                """
                @router.get(
                    "/x",
                    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
                )
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="status-declared-plainly",
            ),
            pytest.param(
                """
                @router.get(
                    "/x",
                    responses=create_openapi_http_exception_doc(
                        [(status.HTTP_404_NOT_FOUND, "not found")]
                    ),
                )
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="status-declared-with-description",
            ),
            pytest.param(
                """
                @router.get("/x")
                def handler():
                    raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid")
                """,
                id="422-is-documented-by-fastapi-itself",
            ),
            pytest.param(
                """
                @router.get("/x")
                def handler():
                    raise HTTPException(status.HTTP_401_UNAUTHORIZED, "who?")
                """,
                id="401-is-declared-on-the-router",
            ),
            pytest.param(
                """
                @router.get("/x")
                def handler():
                    raise HTTPException(status.HTTP_403_FORBIDDEN, "no")
                """,
                id="403-is-declared-on-the-router",
            ),
            pytest.param(
                """
                router = APIRouter(
                    responses={status.HTTP_404_NOT_FOUND: {"description": "not found"}},
                )

                @router.get("/x")
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="router-declares-the-status-for-every-route",
            ),
            pytest.param(
                """
                teams_router = AirflowRouter()

                @teams_router.get(
                    "/x",
                    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
                )
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="airflow-router-without-shared-responses",
            ),
            pytest.param(
                """
                @router.get(
                    "/x",
                    responses={
                        **create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
                        status.HTTP_200_OK: {"description": "ok"},
                    },
                )
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="helper-unpacked-into-a-mapping-alongside-a-success-body",
            ),
            pytest.param(
                """
                router = APIRouter(responses=SHARED_ERRORS)

                @router.get("/x")
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="router-responses-cannot-be-resolved",
            ),
            pytest.param(
                """
                @router.get(
                    "/x",
                    responses={SOME_ALIAS: {"description": "?"}},
                )
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="mapping-key-cannot-be-resolved",
            ),
            pytest.param(
                """
                @router.get("/x", responses={404: {"model": Foo}})
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="unrecognised-responses-shape-is-skipped",
            ),
            pytest.param(
                """
                @router.get("/x", responses=create_openapi_http_exception_doc(SHARED_ERRORS))
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="declared-list-is-not-a-literal",
            ),
            pytest.param(
                """
                @router.get(
                    "/x",
                    responses=create_openapi_http_exception_doc([SOME_ALIAS]),
                )
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="declared-entry-cannot-be-resolved",
            ),
            pytest.param(
                """
                @router.get("/x")
                def handler():
                    raise HTTPException(chosen_status, "nope")
                """,
                id="raised-status-cannot-be-resolved",
            ),
            pytest.param(
                """
                def helper():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="not-a-route-handler",
            ),
            pytest.param(
                """
                @router.websocket("/x")
                def handler():
                    raise HTTPException(status.HTTP_404_NOT_FOUND, "nope")
                """,
                id="not-an-http-route-method",
            ),
            pytest.param(
                """
                @router.get("/x")
                def handler():
                    return 1
                """,
                id="handler-raises-nothing",
            ),
        ],
    )
    def test_no_violation(self, write_python_file, code: str):
        assert check_file(write_python_file(code)) == []

    def test_syntax_error_is_silently_skipped(self, write_python_file):
        assert check_file(write_python_file("def broken(:\n")) == []

    def test_missing_file_is_silently_skipped(self, tmp_path: Path):
        assert check_file(tmp_path / "does_not_exist.py") == []
