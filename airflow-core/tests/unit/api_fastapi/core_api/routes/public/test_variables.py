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
from unittest import mock

import pytest

from airflow.models.variable import Variable
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_variables
from tests_common.test_utils.logs import check_last_log

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
        response = test_client.delete(f"/variables/{TEST_VARIABLE_KEY}")
        assert response.status_code == 204
        variables = session.query(Variable).all()
        assert len(variables) == 3
        check_last_log(session, dag_id=None, event="delete_variable", logical_date=None)

    def test_delete_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.delete(f"/variables/{TEST_VARIABLE_KEY}")
        assert response.status_code == 401

    def test_delete_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.delete(f"/variables/{TEST_VARIABLE_KEY}")
        assert response.status_code == 403

    def test_delete_should_respond_404(self, test_client):
        response = test_client.delete(f"/variables/{TEST_VARIABLE_KEY}")
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
        response = test_client.get(f"/variables/{key}")
        assert response.status_code == 200
        assert response.json() == expected_response

    def test_get_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/variables/{TEST_VARIABLE_KEY}")
        assert response.status_code == 401

    def test_get_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/variables/{TEST_VARIABLE_KEY}")
        assert response.status_code == 403

    def test_get_should_respond_404(self, test_client):
        response = test_client.get(f"/variables/{TEST_VARIABLE_KEY}")
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
        response = test_client.get("/variables", params=query_params)

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert [variable["key"] for variable in body["variables"]] == expected_keys

    def test_get_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/variables")
        assert response.status_code == 401

    def test_get_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/variables")
        assert response.status_code == 403


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
        response = test_client.patch(f"/variables/{key}", json=body, params=params)
        assert response.status_code == 200
        assert response.json() == expected_response
        check_last_log(session, dag_id=None, event="patch_variable", logical_date=None)

    def test_patch_should_respond_400(self, test_client):
        response = test_client.patch(
            f"/variables/{TEST_VARIABLE_KEY}",
            json={"key": "different_key", "value": "some_value", "description": None},
        )
        assert response.status_code == 400
        body = response.json()
        assert body["detail"] == "Invalid body, key from request body doesn't match uri parameter"

    def test_patch_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(
            f"/variables/{TEST_VARIABLE_KEY}",
            json={"key": TEST_VARIABLE_KEY, "value": "some_value", "description": None},
        )
        assert response.status_code == 401

    def test_patch_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(
            f"/variables/{TEST_VARIABLE_KEY}",
            json={"key": TEST_VARIABLE_KEY, "value": "some_value", "description": None},
        )
        assert response.status_code == 403

    def test_patch_should_respond_404(self, test_client):
        response = test_client.patch(
            f"/variables/{TEST_VARIABLE_KEY}",
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
        response = test_client.post("/variables", json=body)
        assert response.status_code == 201
        assert response.json() == expected_response
        check_last_log(session, dag_id=None, event="post_variable", logical_date=None)

    def test_post_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            "/variables",
            json={
                "key": "new variable key",
                "value": "new variable value",
                "description": "new variable description",
            },
        )
        assert response.status_code == 401

    def test_post_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(
            "/variables",
            json={
                "key": "new variable key",
                "value": "new variable value",
                "description": "new variable description",
            },
        )
        assert response.status_code == 403

    def test_post_should_respond_409_when_key_exists(self, test_client, session):
        self.create_variables()
        # Attempting to post a variable with an existing key
        response = test_client.post(
            "/variables",
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
        response = test_client.post("/variables", json=body)
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
        response = test_client.post("/variables", json=body)
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

    @pytest.mark.parametrize(
        "body",
        [
            {
                "key": "new variable key",
                "value": "new variable value",
                "description": "new variable description",
            },
        ],
    )
    @mock.patch("airflow.api_fastapi.logging.decorators._mask_variable_fields")
    def test_mask_variable_fields_called(self, mock_mask_variable_fields, test_client, body):
        mock_mask_variable_fields.return_value = {**body, "method": "POST"}
        response = test_client.post("/variables", json=body)
        assert response.status_code == 201

        mock_mask_variable_fields.assert_called_once_with(body)


class TestBulkVariables(TestVariableEndpoint):
    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        "actions, expected_results",
        [
            # Test successful create
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {"key": "new_var1", "value": "new_value1", "description": "New variable 1"},
                                {"key": "new_var2", "value": "new_value2", "description": "New variable 2"},
                            ],
                            "action_on_existence": "fail",
                        }
                    ]
                },
                {"create": {"success": ["new_var1", "new_var2"], "errors": []}},
            ),
            # Test successful create with skip
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "key": "test_variable_key",
                                    "value": "new_value1",
                                    "description": "New variable 1",
                                },
                                {"key": "new_var2", "value": "new_value2", "description": "New variable 2"},
                            ],
                            "action_on_existence": "skip",
                        }
                    ]
                },
                {"create": {"success": ["new_var2"], "errors": []}},
            ),
            # Test successful create with overwrite
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "key": "test_variable_key",
                                    "value": "new_value1",
                                    "description": "New variable 1",
                                },
                                {"key": "new_var2", "value": "new_value2", "description": "New variable 2"},
                            ],
                            "action_on_existence": "overwrite",
                        }
                    ]
                },
                {"create": {"success": ["test_variable_key", "new_var2"], "errors": []}},
            ),
            # Test create conflict
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "key": "test_variable_key",
                                    "value": "new_value",
                                    "description": "Should conflict",
                                }
                            ],
                            "action_on_existence": "fail",
                        }
                    ]
                },
                {
                    "create": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The variables with these keys: {'test_variable_key'} already exist.",
                                "status_code": 409,
                            }
                        ],
                    }
                },
            ),
            # Test successful update
            (
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "key": "test_variable_key",
                                    "value": "updated_value",
                                    "description": "Updated variable 1",
                                }
                            ],
                            "action_on_non_existence": "fail",
                        }
                    ]
                },
                {"update": {"success": ["test_variable_key"], "errors": []}},
            ),
            # Test update with skip
            (
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "key": "key_not_present",
                                    "value": "updated_value",
                                    "description": "Updated variable 1",
                                }
                            ],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {"update": {"success": [], "errors": []}},
            ),
            # Test update not found
            (
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "key": "nonexistent_var",
                                    "value": "updated_value",
                                    "description": "Should fail",
                                }
                            ],
                            "action_on_non_existence": "fail",
                        }
                    ]
                },
                {
                    "update": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The variables with these keys: {'nonexistent_var'} were not found.",
                                "status_code": 404,
                            }
                        ],
                    }
                },
            ),
            # Test successful delete
            (
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": ["test_variable_key"],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {"delete": {"success": ["test_variable_key"], "errors": []}},
            ),
            # Test delete with skip
            (
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": ["key_not_present"],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {"delete": {"success": [], "errors": []}},
            ),
            # Test delete not found
            (
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": ["nonexistent_var"],
                            "action_on_non_existence": "fail",
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The variables with these keys: {'nonexistent_var'} were not found.",
                                "status_code": 404,
                            }
                        ],
                    }
                },
            ),
            # Test Create, Update, and Delete combined
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [{"key": "new_var1", "value": "new_value1"}],
                            "action_on_existence": "skip",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "key": "test_variable_key",
                                    "value": "updated_value",
                                    "description": "Updated variable 1",
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                        {
                            "action": "delete",
                            "entities": ["dictionary_password"],
                            "action_on_non_existence": "skip",
                        },
                    ]
                },
                {
                    "create": {"success": ["new_var1"], "errors": []},
                    "update": {"success": ["test_variable_key"], "errors": []},
                    "delete": {"success": ["dictionary_password"], "errors": []},
                },
            ),
            # Test Fail on conflicting create and handle others
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "key": "test_variable_key",
                                    "value": "new_value",
                                    "description": "Should conflict",
                                }
                            ],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "key": "dictionary_password",
                                    "value": "updated_value",
                                    "description": "Updated variable 2",
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                        {
                            "action": "delete",
                            "entities": ["nonexistent_var"],
                            "action_on_non_existence": "skip",
                        },
                    ]
                },
                {
                    "create": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The variables with these keys: {'test_variable_key'} already exist.",
                                "status_code": 409,
                            }
                        ],
                    },
                    "update": {"success": ["dictionary_password"], "errors": []},
                    "delete": {"success": [], "errors": []},
                },
            ),
            # Test all skipping actions
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "key": "test_variable_key",
                                    "value": "new_value1",
                                    "description": "Should be skipped",
                                }
                            ],
                            "action_on_existence": "skip",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "key": "nonexistent_var",
                                    "value": "updated_value",
                                    "description": "Should be skipped",
                                }
                            ],
                            "action_on_non_existence": "skip",
                        },
                        {
                            "action": "delete",
                            "entities": ["nonexistent_var"],
                            "action_on_non_existence": "skip",
                        },
                    ]
                },
                {
                    "create": {"success": [], "errors": []},
                    "update": {"success": [], "errors": []},
                    "delete": {"success": [], "errors": []},
                },
            ),
            # Test Dependent actions
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "key": "new_variable_key",
                                    "value": "new_value1",
                                    "description": "test case description",
                                }
                            ],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "key": "new_variable_key",
                                    "value": "updated_value",
                                    "description": "description updated",
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                        {
                            "action": "delete",
                            "entities": ["new_variable_key"],
                            "action_on_non_existence": "fail",
                        },
                    ]
                },
                {
                    "create": {"success": ["new_variable_key"], "errors": []},
                    "update": {"success": ["new_variable_key"], "errors": []},
                    "delete": {"success": ["new_variable_key"], "errors": []},
                },
            ),
            # Test Repeated actions
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "key": "test_variable_key",
                                    "value": "new_value1",
                                    "description": "test case description",
                                }
                            ],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "key": "test_variable_key",
                                    "value": "updated_value",
                                    "description": "description updated",
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "key": "new_key",
                                    "value": "new_value1",
                                    "description": "test case description",
                                }
                            ],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "key": "nonexistent_var",
                                    "value": "updated_value",
                                    "description": "description updated",
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                        {
                            "action": "delete",
                            "entities": ["dictionary_password"],
                            "action_on_non_existence": "fail",
                        },
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "key": "new_variable_key_1",
                                    "value": "new_value1",
                                    "description": "test case description",
                                }
                            ],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "key": "new_variable_key",
                                    "value": "updated_value",
                                    "description": "description updated",
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                    ]
                },
                {
                    "create": {
                        "success": ["new_key", "new_variable_key_1"],
                        "errors": [
                            {
                                "error": "The variables with these keys: {'test_variable_key'} already exist.",
                                "status_code": 409,
                            }
                        ],
                    },
                    "update": {
                        "success": ["test_variable_key"],
                        "errors": [
                            {
                                "error": "The variables with these keys: {'nonexistent_var'} were not found.",
                                "status_code": 404,
                            },
                            {
                                "error": "The variables with these keys: {'new_variable_key'} were not found.",
                                "status_code": 404,
                            },
                        ],
                    },
                    "delete": {"success": ["dictionary_password"], "errors": []},
                },
            ),
        ],
    )
    def test_bulk_variables(self, test_client, actions, expected_results, session):
        self.create_variables()
        response = test_client.patch("/variables", json=actions)
        response_data = response.json()
        for key, value in expected_results.items():
            assert response_data[key] == value
        check_last_log(session, dag_id=None, event="bulk_variables", logical_date=None)

    def test_bulk_variables_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch("/variables", json={})
        assert response.status_code == 401

    def test_bulk_variables_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch("/variables", json={})
        assert response.status_code == 403
