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

pytestmark = pytest.mark.db_test


@pytest.fixture
def old_ver_client(client):
    client.headers["Airflow-API-Version"] = "2026-03-31"
    return client


def test_dag_endpoint_not_available_in_previous_version(old_ver_client):
    response = old_ver_client.get("/execution/dags/test_dag")

    assert response.status_code == 404
