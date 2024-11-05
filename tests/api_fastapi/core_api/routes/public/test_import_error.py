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

from datetime import datetime, timezone

import pytest

from airflow.models.errors import ParseImportError
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_import_errors

pytestmark = pytest.mark.db_test

FILENAME1 = "test_filename1.py"
FILENAME2 = "test_filename2.py"
FILENAME3 = "Lorem_ipsum.py"
STACKTRACE1 = "test_stacktrace1"
STACKTRACE2 = "test_stacktrace2"
STACKTRACE3 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
TIMESTAMP1 = datetime(2024, 6, 15, 1, 0, tzinfo=timezone.utc)
TIMESTAMP2 = datetime(2024, 6, 15, 5, 0, tzinfo=timezone.utc)
TIMESTAMP3 = datetime(2024, 6, 15, 3, 0, tzinfo=timezone.utc)
IMPORT_ERROR_NON_EXISTED_ID = 9999
IMPORT_ERROR_NON_EXISTED_KEY = "non_existed_key"


class TestImportErrorEndpoint:
    """Common class for /public/importErrors related unit tests."""

    @staticmethod
    def _clear_db():
        clear_db_import_errors()

    @pytest.fixture(autouse=True)
    @provide_session
    def setup(self, session=None) -> dict[str, ParseImportError]:
        """
        Setup method which is run before every test.
        """
        self._clear_db()
        import_error1 = ParseImportError(
            filename=FILENAME1,
            stacktrace=STACKTRACE1,
            timestamp=TIMESTAMP1,
        )
        import_error2 = ParseImportError(
            filename=FILENAME2,
            stacktrace=STACKTRACE2,
            timestamp=TIMESTAMP2,
        )
        import_error3 = ParseImportError(
            filename=FILENAME3,
            stacktrace=STACKTRACE3,
            timestamp=TIMESTAMP3,
        )
        session.add_all([import_error1, import_error2, import_error3])
        session.commit()
        return {FILENAME1: import_error1, FILENAME2: import_error2, FILENAME3: import_error3}

    def teardown_method(self) -> None:
        self._clear_db()


class TestGetImportError(TestImportErrorEndpoint):
    @pytest.mark.parametrize(
        "import_error_key, expected_status_code, expected_body",
        [
            (
                FILENAME1,
                200,
                {
                    "import_error_id": 1,
                    "timestamp": TIMESTAMP1,
                    "filename": FILENAME1,
                    "stack_trace": STACKTRACE1,
                },
            ),
            (
                FILENAME2,
                200,
                {
                    "import_error_id": 2,
                    "timestamp": TIMESTAMP2,
                    "filename": FILENAME2,
                    "stack_trace": STACKTRACE2,
                },
            ),
            (IMPORT_ERROR_NON_EXISTED_KEY, 404, {}),
        ],
    )
    def test_get_import_error(
        self, test_client, setup, import_error_key, expected_status_code, expected_body
    ):
        import_error: ParseImportError | None = setup.get(import_error_key)
        import_error_id = import_error.id if import_error else IMPORT_ERROR_NON_EXISTED_ID
        response = test_client.get(f"/public/importErrors/{import_error_id}")
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return
        expected_json = {
            "import_error_id": import_error_id,
            "timestamp": expected_body["timestamp"].isoformat().replace("+00:00", "Z"),
            "filename": expected_body["filename"],
            "stack_trace": expected_body["stack_trace"],
        }
        assert response.json() == expected_json


class TestGetImportErrors(TestImportErrorEndpoint):
    @pytest.mark.parametrize(
        "query_params, expected_status_code, expected_total_entries, expected_filenames",
        [
            (
                {},
                200,
                3,
                [FILENAME1, FILENAME2, FILENAME3],
            ),
            # offset, limit
            (
                {"limit": 1, "offset": 1},
                200,
                3,
                [FILENAME2],
            ),
            (
                {"limit": 1, "offset": 2},
                200,
                3,
                [FILENAME3],
            ),
            # order_by
            (
                {"order_by": "-filename"},
                200,
                3,
                [FILENAME2, FILENAME1, FILENAME3],
            ),
            (
                {"order_by": "timestamp"},
                200,
                3,
                [FILENAME1, FILENAME3, FILENAME2],
            ),
            (
                {"order_by": "import_error_id"},
                200,
                3,
                [FILENAME1, FILENAME2, FILENAME3],
            ),
            (
                {"order_by": "-import_error_id"},
                200,
                3,
                [FILENAME3, FILENAME2, FILENAME1],
            ),
            # invalid order_by
            (
                {"order_by": "invalid_order_by"},
                400,
                0,
                [],
            ),
            # combination of query parameters
            (
                {"limit": 2, "offset": 1, "order_by": "-filename"},
                200,
                3,
                [FILENAME1, FILENAME3],
            ),
            (
                {"limit": 1, "offset": 2, "order_by": "-filename"},
                200,
                3,
                [FILENAME3],
            ),
            (
                {"limit": 5, "offset": 1, "order_by": "timestamp"},
                200,
                3,
                [FILENAME3, FILENAME2],
            ),
        ],
    )
    def test_get_import_errors(
        self,
        test_client,
        query_params,
        expected_status_code,
        expected_total_entries,
        expected_filenames,
    ):
        response = test_client.get("/public/importErrors", params=query_params)

        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        response_json = response.json()
        assert response_json["total_entries"] == expected_total_entries
        assert [
            import_error["filename"] for import_error in response_json["import_errors"]
        ] == expected_filenames
