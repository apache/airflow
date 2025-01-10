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
from io import BytesIO

import pytest

from airflow.models.variable import Variable
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_variables

pytestmark = pytest.mark.db_test

TEST_VARIABLE_KEY = "test_variable_key"
TEST_VARIABLE_VALUE = "test_variable_value"
TEST_VARIABLE_DESCRIPTION = "Some description for the variable"


TEST_VARIABLE_KEY2 = "password"
TEST_VARIABLE_VALUE2 = "some_password"
TEST_VARIABLE_DESCRIPTION2 = "Some description for the password"


TEST_VARIABLE_KEY3 = "dictionary_password"
TEST_VARIABLE_VALUE3 = '{"password": "some_password"}'
TEST_VARIABLE_DESCRIPTION3 = "Some description for the variable"


TEST_VARIABLE_SEARCH_KEY = "test_variable_search_key"
TEST_VARIABLE_SEARCH_VALUE = "random search value"
TEST_VARIABLE_SEARCH_DESCRIPTION = "Some description for the variable"


# Helper function to simulate file upload
def create_file_upload(content: dict) -> BytesIO:
    return BytesIO(json.dumps(content).encode("utf-8"))


@provide_session
def _create_variables(session) -> None:
    Variable.set(
        key=TEST_VARIABLE_KEY,
        value=TEST_VARIABLE_VALUE,
        description=TEST_VARIABLE_DESCRIPTION,
        session=session,
    )

    Variable.set(
        key=TEST_VARIABLE_KEY2,
        value=TEST_VARIABLE_VALUE2,
        description=TEST_VARIABLE_DESCRIPTION2,
        session=session,
    )

    Variable.set(
        key=TEST_VARIABLE_KEY3,
        value=TEST_VARIABLE_VALUE3,
        description=TEST_VARIABLE_DESCRIPTION3,
        session=session,
    )

    Variable.set(
        key=TEST_VARIABLE_SEARCH_KEY,
        value=TEST_VARIABLE_SEARCH_VALUE,
        description=TEST_VARIABLE_SEARCH_DESCRIPTION,
        session=session,
    )


class TestVariableEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_variables()

    def teardown_method(self) -> None:
        clear_db_variables()

    def create_variables(self):
        _create_variables()


class TestDeleteVariable(TestVariableEndpoint):
    def test_delete_should_respond_204(self, test_client, session):
        self.create_variables()
        variables = session.query(Variable).all()
        assert len(variables) == 4
        response = test_client.delete(f"/public/variables/{TEST_VARIABLE_KEY}")
        assert response.status_code == 204
        variables = session.query(Variable).all()
        assert len(variables) == 3

    def test_delete_should_respond_404(self, test_client):
        response = test_client.delete(f"/public/variables/{TEST_VARIABLE_KEY}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Variable with key: `{TEST_VARIABLE_KEY}` was not found" == body["detail"]


class TestGetVariable(TestVariableEndpoint):
    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        "key, expected_response",
        [
            (
                TEST_VARIABLE_KEY,
                {
                    "key": TEST_VARIABLE_KEY,
                    "value": TEST_VARIABLE_VALUE,
                    "description": TEST_VARIABLE_DESCRIPTION,
                    "is_encrypted": True,
                },
            ),
            (
                TEST_VARIABLE_KEY2,
                {
                    "key": TEST_VARIABLE_KEY2,
                    "value": "***",
                    "description": TEST_VARIABLE_DESCRIPTION2,
                    "is_encrypted": True,
                },
            ),
            (
                TEST_VARIABLE_KEY3,
                {
                    "key": TEST_VARIABLE_KEY3,
                    "value": '{"password": "***"}',
                    "description": TEST_VARIABLE_DESCRIPTION3,
                    "is_encrypted": True,
                },
            ),
            (
                TEST_VARIABLE_SEARCH_KEY,
                {
                    "key": TEST_VARIABLE_SEARCH_KEY,
                    "value": TEST_VARIABLE_SEARCH_VALUE,
                    "description": TEST_VARIABLE_SEARCH_DESCRIPTION,
                    "is_encrypted": True,
                },
            ),
        ],
    )
    def test_get_should_respond_200(self, test_client, session, key, expected_response):
        self.create_variables()
        response = test_client.get(f"/public/variables/{key}")
        assert response.status_code == 200
        assert response.json() == expected_response

    def test_get_should_respond_404(self, test_client):
        response = test_client.get(f"/public/variables/{TEST_VARIABLE_KEY}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Variable with key: `{TEST_VARIABLE_KEY}` was not found" == body["detail"]


class TestGetVariables(TestVariableEndpoint):
    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        "query_params, expected_total_entries, expected_keys",
        [
            # Filters
            ({}, 4, [TEST_VARIABLE_KEY, TEST_VARIABLE_KEY2, TEST_VARIABLE_KEY3, TEST_VARIABLE_SEARCH_KEY]),
            ({"limit": 1}, 4, [TEST_VARIABLE_KEY]),
            ({"limit": 1, "offset": 1}, 4, [TEST_VARIABLE_KEY2]),
            # Sort
            (
                {"order_by": "id"},
                4,
                [TEST_VARIABLE_KEY, TEST_VARIABLE_KEY2, TEST_VARIABLE_KEY3, TEST_VARIABLE_SEARCH_KEY],
            ),
            (
                {"order_by": "-id"},
                4,
                [TEST_VARIABLE_SEARCH_KEY, TEST_VARIABLE_KEY3, TEST_VARIABLE_KEY2, TEST_VARIABLE_KEY],
            ),
            (
                {"order_by": "key"},
                4,
                [TEST_VARIABLE_KEY3, TEST_VARIABLE_KEY2, TEST_VARIABLE_KEY, TEST_VARIABLE_SEARCH_KEY],
            ),
            (
                {"order_by": "-key"},
                4,
                [TEST_VARIABLE_SEARCH_KEY, TEST_VARIABLE_KEY, TEST_VARIABLE_KEY2, TEST_VARIABLE_KEY3],
            ),
            # Search
            (
                {"variable_key_pattern": "~"},
                4,
                [TEST_VARIABLE_KEY, TEST_VARIABLE_KEY2, TEST_VARIABLE_KEY3, TEST_VARIABLE_SEARCH_KEY],
            ),
            ({"variable_key_pattern": "search"}, 1, [TEST_VARIABLE_SEARCH_KEY]),
        ],
    )
    def test_should_respond_200(
        self, session, test_client, query_params, expected_total_entries, expected_keys
    ):
        self.create_variables()
        response = test_client.get("/public/variables", params=query_params)

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert [variable["key"] for variable in body["variables"]] == expected_keys


class TestPatchVariable(TestVariableEndpoint):
    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        "key, body, params, expected_response",
        [
            (
                TEST_VARIABLE_KEY,
                {
                    "key": TEST_VARIABLE_KEY,
                    "value": "The new value",
                    "description": "The new description",
                },
                None,
                {
                    "key": TEST_VARIABLE_KEY,
                    "value": "The new value",
                    "description": "The new description",
                    "is_encrypted": True,
                },
            ),
            (
                TEST_VARIABLE_KEY,
                {
                    "key": TEST_VARIABLE_KEY,
                    "value": "The new value",
                    "description": "The new description",
                },
                {"update_mask": ["value"]},
                {
                    "key": TEST_VARIABLE_KEY,
                    "value": "The new value",
                    "description": TEST_VARIABLE_DESCRIPTION,
                    "is_encrypted": True,
                },
            ),
            (
                TEST_VARIABLE_KEY2,
                {
                    "key": TEST_VARIABLE_KEY2,
                    "value": "some_other_value",
                    "description": TEST_VARIABLE_DESCRIPTION2,
                },
                None,
                {
                    "key": TEST_VARIABLE_KEY2,
                    "value": "***",
                    "description": TEST_VARIABLE_DESCRIPTION2,
                    "is_encrypted": True,
                },
            ),
            (
                TEST_VARIABLE_KEY3,
                {
                    "key": TEST_VARIABLE_KEY3,
                    "value": '{"password": "new_password"}',
                    "description": "new description",
                },
                {"update_mask": ["value", "description"]},
                {
                    "key": TEST_VARIABLE_KEY3,
                    "value": '{"password": "***"}',
                    "description": "new description",
                    "is_encrypted": True,
                },
            ),
        ],
    )
    def test_patch_should_respond_200(self, test_client, session, key, body, params, expected_response):
        self.create_variables()
        response = test_client.patch(f"/public/variables/{key}", json=body, params=params)
        assert response.status_code == 200
        assert response.json() == expected_response

    def test_patch_should_respond_400(self, test_client):
        response = test_client.patch(
            f"/public/variables/{TEST_VARIABLE_KEY}",
            json={"key": "different_key", "value": "some_value", "description": None},
        )
        assert response.status_code == 400
        body = response.json()
        assert body["detail"] == "Invalid body, key from request body doesn't match uri parameter"

    def test_patch_should_respond_404(self, test_client):
        response = test_client.patch(
            f"/public/variables/{TEST_VARIABLE_KEY}",
            json={"key": TEST_VARIABLE_KEY, "value": "some_value", "description": None},
        )
        assert response.status_code == 404
        body = response.json()
        assert f"The Variable with key: `{TEST_VARIABLE_KEY}` was not found" == body["detail"]


class TestPostVariable(TestVariableEndpoint):
    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        "body, expected_response",
        [
            (
                {
                    "key": "new variable key",
                    "value": "new variable value",
                    "description": "new variable description",
                },
                {
                    "key": "new variable key",
                    "value": "new variable value",
                    "description": "new variable description",
                    "is_encrypted": True,
                },
            ),
            (
                {
                    "key": "another_password",
                    "value": "password_value",
                    "description": "another password",
                },
                {
                    "key": "another_password",
                    "value": "***",
                    "description": "another password",
                    "is_encrypted": True,
                },
            ),
            (
                {
                    "key": "another value with sensitive information",
                    "value": '{"password": "new_password"}',
                    "description": "some description",
                },
                {
                    "key": "another value with sensitive information",
                    "value": '{"password": "***"}',
                    "description": "some description",
                    "is_encrypted": True,
                },
            ),
            (
                {
                    "key": "empty value variable",
                    "value": "",
                    "description": "some description",
                },
                {
                    "key": "empty value variable",
                    "value": "",
                    "description": "some description",
                    "is_encrypted": True,
                },
            ),
        ],
    )
    def test_post_should_respond_201(self, test_client, session, body, expected_response):
        self.create_variables()
        response = test_client.post("/public/variables", json=body)
        assert response.status_code == 201
        assert response.json() == expected_response

    def test_post_should_respond_409_when_key_exists(self, test_client, session):
        self.create_variables()
        # Attempting to post a variable with an existing key
        response = test_client.post(
            "/public/variables",
            json={
                "key": TEST_VARIABLE_KEY,
                "value": "duplicate value",
                "description": "duplicate description",
            },
        )
        assert response.status_code == 409
        body = response.json()
        assert body["detail"] == f"The Variable with key: `{TEST_VARIABLE_KEY}` already exists"

    def test_post_should_respond_422_when_key_too_large(self, test_client):
        large_key = "a" * 251  # Exceeds the maximum length of 250
        body = {
            "key": large_key,
            "value": "some_value",
            "description": "key too large",
        }
        response = test_client.post("/public/variables", json=body)
        assert response.status_code == 422
        assert response.json() == {
            "detail": [
                {
                    "type": "string_too_long",
                    "loc": ["body", "key"],
                    "msg": "String should have at most 250 characters",
                    "input": large_key,
                    "ctx": {"max_length": 250},
                }
            ]
        }

    def test_post_should_respond_422_when_value_is_null(self, test_client):
        body = {
            "key": "null value key",
            "value": None,
            "description": "key too large",
        }
        response = test_client.post("/public/variables", json=body)
        assert response.status_code == 422
        assert response.json() == {
            "detail": [
                {
                    "type": "string_type",
                    "loc": ["body", "value"],
                    "msg": "Input should be a valid string",
                    "input": None,
                }
            ]
        }


class TestImportVariables(TestVariableEndpoint):
    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        "variables_data, behavior, expected_status_code, expected_created_count, expected_created_keys, expected_conflict_keys",
        [
            (
                {"new_key1": "new_value1", "new_key2": "new_value2"},
                "overwrite",
                200,
                2,
                {"new_key1", "new_key2"},
                set(),
            ),
            (
                {"new_key1": "new_value1", "new_key2": "new_value2"},
                "skip",
                200,
                2,
                {"new_key1", "new_key2"},
                set(),
            ),
            (
                {"test_variable_key": "new_value", "new_key": "new_value"},
                "fail",
                409,
                0,
                set(),
                {"test_variable_key"},
            ),
            (
                {"test_variable_key": "new_value", "new_key": "new_value"},
                "skip",
                200,
                1,
                {"new_key"},
                {"test_variable_key"},
            ),
            (
                {"test_variable_key": "new_value", "new_key": "new_value"},
                "overwrite",
                200,
                2,
                {"test_variable_key", "new_key"},
                set(),
            ),
        ],
    )
    def test_import_variables(
        self,
        test_client,
        variables_data,
        behavior,
        expected_status_code,
        expected_created_count,
        expected_created_keys,
        expected_conflict_keys,
        session,
    ):
        """Test variable import with different behaviors (overwrite, fail, skip)."""

        self.create_variables()

        file = create_file_upload(variables_data)
        response = test_client.post(
            "/public/variables/import",
            files={"file": ("variables.json", file, "application/json")},
            params={"action_if_exists": behavior},
        )

        assert response.status_code == expected_status_code

        if expected_status_code == 200:
            body = response.json()
            assert body["created_count"] == expected_created_count
            assert set(body["created_variable_keys"]) == expected_created_keys

        elif expected_status_code == 409:
            body = response.json()
            assert (
                f"The variables with these keys: {expected_conflict_keys} already exists." == body["detail"]
            )

    def test_import_invalid_json(self, test_client):
        """Test invalid JSON import."""
        file = BytesIO(b"import variable test")
        response = test_client.post(
            "/public/variables/import",
            files={"file": ("variables.json", file, "application/json")},
            params={"action_if_exists": "overwrite"},
        )

        assert response.status_code == 400
        assert "Invalid JSON format" in response.json()["detail"]

    def test_import_empty_file(self, test_client):
        """Test empty file import."""
        file = create_file_upload({})
        response = test_client.post(
            "/public/variables/import",
            files={"file": ("empty_variables.json", file, "application/json")},
            params={"action_if_exists": "overwrite"},
        )

        assert response.status_code == 422
        assert response.json()["detail"] == "No variables found in the provided JSON."
