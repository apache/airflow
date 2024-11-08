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

from unittest import mock

import pytest

from airflow.models.variable import Variable


class TestGetVariable:
    @pytest.mark.db_test
    def test_variable_get_from_db(self, client, session):
        Variable.set(key="var1", value="value", session=session)
        session.commit()

        response = client.get("/execution/variable/var1")

        assert response.status_code == 200
        assert response.json() == {"key": "var1", "value": "value"}

        # Remove connection
        Variable.delete(key="var1", session=session)
        session.commit()

    @mock.patch.dict(
        "os.environ",
        {"AIRFLOW_VAR_KEY1": "VALUE"},
    )
    def test_variable_get_from_env_var(self, client, session):
        response = client.get("/execution/variable/key1")

        assert response.status_code == 200
        assert response.json() == {"key": "key1", "value": "VALUE"}

    def test_variable_get_not_found(self, client):
        response = client.get("/execution/variable/non_existent_var")

        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "Variable with key 'non_existent_var' not found",
                "reason": "not_found",
            }
        }

    def test_variable_get_access_denied(self, client):
        with mock.patch(
            "airflow.api_fastapi.execution_api.routes.variables.has_variable_access", return_value=False
        ):
            response = client.get("/execution/variable/key1")

        # Assert response status code and detail for access denied
        assert response.status_code == 403
        assert response.json() == {
            "detail": {
                "reason": "access_denied",
                "message": "Task does not have access to variable key1",
            }
        }
