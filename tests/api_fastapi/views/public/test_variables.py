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

from airflow.models.variable import Variable
from airflow.utils.session import provide_session

from dev.tests_common.test_utils.db import clear_db_variables

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


@provide_session
def _create_variable(session) -> None:
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


class TestVariableEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_variables()

    def teardown_method(self) -> None:
        clear_db_variables()

    def create_variable(self):
        _create_variable()


class TestDeleteVariable(TestVariableEndpoint):
    def test_delete_should_respond_204(self, test_client, session):
        self.create_variable()
        variables = session.query(Variable).all()
        assert len(variables) == 3
        response = test_client.delete(f"/public/variables/{TEST_VARIABLE_KEY}")
        assert response.status_code == 204
        variables = session.query(Variable).all()
        assert len(variables) == 2

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
                },
            ),
            (
                TEST_VARIABLE_KEY2,
                {
                    "key": TEST_VARIABLE_KEY2,
                    "value": "***",
                    "description": TEST_VARIABLE_DESCRIPTION2,
                },
            ),
            (
                TEST_VARIABLE_KEY3,
                {
                    "key": TEST_VARIABLE_KEY3,
                    "value": '{"password": "***"}',
                    "description": TEST_VARIABLE_DESCRIPTION3,
                },
            ),
        ],
    )
    def test_get_should_respond_200(self, test_client, session, key, expected_response):
        self.create_variable()
        response = test_client.get(f"/public/variables/{key}")
        assert response.status_code == 200
        assert response.json() == expected_response

    def test_get_should_respond_404(self, test_client):
        response = test_client.get(f"/public/variables/{TEST_VARIABLE_KEY}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Variable with key: `{TEST_VARIABLE_KEY}` was not found" == body["detail"]
