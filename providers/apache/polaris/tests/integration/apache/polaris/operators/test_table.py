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

import pytest

from airflow.providers.apache.polaris.hooks.polaris import PolarisCatalogHook
from airflow.providers.apache.polaris.operators.table import PolarisCreateTableOperator

from integration.apache.polaris import CLIENT_SECRET

pytestmark = pytest.mark.integration("polaris")


def test_create_table_returns_a_summary_without_credentials(conn, catalog_name):
    conn_id = conn(default_catalog=catalog_name)
    PolarisCatalogHook(conn_id).create_namespace("sales")
    result = PolarisCreateTableOperator(
        task_id="t",
        namespace="sales",
        table="orders",
        schema={"type": "struct", "fields": [{"id": 1, "name": "id", "type": "long", "required": False}]},
        polaris_conn_id=conn_id,
    ).execute({})
    assert set(result) == {
        "namespace",
        "name",
        "table_uuid",
        "location",
        "metadata_location",
        "format_version",
    }
    assert f"/tmp/polaris/{catalog_name}/sales/orders/metadata/" in result["metadata_location"]
    assert CLIENT_SECRET not in json.dumps(result)
