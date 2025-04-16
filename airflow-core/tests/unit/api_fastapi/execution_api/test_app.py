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

from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance
from airflow.api_fastapi.execution_api.versions import bundle

pytestmark = pytest.mark.db_test


def test_custom_openapi_includes_extra_schemas(client):
    """Test to ensure that extra schemas are correctly included in the OpenAPI schema."""
    response = client.get("/execution/openapi.json?version=2025-04-11")
    assert response.status_code == 200

    openapi_schema = response.json()

    assert "TaskInstance" in openapi_schema["components"]["schemas"]
    schema = openapi_schema["components"]["schemas"]["TaskInstance"]

    assert schema == TaskInstance.model_json_schema()


def test_access_api_contract(client):
    response = client.get("/execution/docs")
    assert response.status_code == 200
    assert response.headers["airflow-api-version"] == bundle.versions[0].value
