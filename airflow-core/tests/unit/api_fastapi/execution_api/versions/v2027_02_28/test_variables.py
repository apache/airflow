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
from sqlalchemy import select

from airflow.models.variable import Variable

from tests_common.test_utils.db import clear_db_variables

pytestmark = pytest.mark.db_test

PREVIOUS_VERSION = "2026-10-30"


@pytest.fixture(autouse=True)
def setup_method():
    clear_db_variables()
    yield
    clear_db_variables()


class TestVariableValueNonNullableVersioning:
    """The Variable value became non-nullable in the 2027-02-28 API version."""

    @pytest.mark.parametrize("body", [{"value": None}, {"val": None}])
    def test_old_version_put_null_stores_none_string(self, client, session, body):
        client.headers["Airflow-API-Version"] = PREVIOUS_VERSION

        response = client.put("/execution/variables/var_null", json=body)

        assert response.status_code == 201, response.json()
        var = session.scalars(select(Variable).where(Variable.key == "var_null")).one()
        assert var.val == "None"

    def test_head_put_null_returns_422(self, client):
        response = client.put("/execution/variables/var_null", json={"value": None})

        assert response.status_code == 422
        assert response.json()["detail"][0]["type"] == "string_type"

    @pytest.mark.parametrize("version", [PREVIOUS_VERSION, None])
    def test_get_returns_string(self, client, session, version):
        if version:
            client.headers["Airflow-API-Version"] = version
        Variable.set(key="var1", value="value", session=session)
        session.commit()

        response = client.get("/execution/variables/var1")

        assert response.status_code == 200
        assert response.json() == {"key": "var1", "value": "value"}

    @pytest.mark.parametrize("version", [PREVIOUS_VERSION, None])
    def test_get_missing_returns_404(self, client, version):
        if version:
            client.headers["Airflow-API-Version"] = version

        response = client.get("/execution/variables/missing")

        assert response.status_code == 404
