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
from datetime import datetime

from apache_polaris.sdk.management import Catalog

from airflow.providers.apache.polaris.operators.catalog import (
    PolarisCreateCatalogOperator,
    PolarisDeleteCatalogOperator,
    PolarisUpdateCatalogOperator,
)
from airflow.providers.common.compat.sdk import DAG

from tests_common.test_utils.compat import DagSerialization

CATALOG = {
    "type": "INTERNAL",
    "name": "analytics",
    "properties": {"default-base-location": "s3://lake/analytics"},
    "storageConfigInfo": {"storageType": "S3", "roleArn": "arn:aws:iam::1:role/r"},
}


class TestCatalogOperators:
    def test_create_defaults_to_skip_and_returns_dict(self, management_hook):
        management_hook.create_catalog.return_value = Catalog.from_dict({**CATALOG, "entityVersion": 1})
        result = PolarisCreateCatalogOperator(task_id="t", catalog=CATALOG).execute({})
        management_hook.create_catalog.assert_called_once_with(CATALOG, if_exists="skip")
        assert result["name"] == "analytics"
        assert result["entityVersion"] == 1
        json.dumps(result)

    def test_update(self, management_hook):
        management_hook.update_catalog.return_value = Catalog.from_dict(CATALOG)
        PolarisUpdateCatalogOperator(
            task_id="t", catalog_name="analytics", properties={"a": "1"}, replace_properties=True
        ).execute({})
        management_hook.update_catalog.assert_called_once_with(
            "analytics", properties={"a": "1"}, storage_config_info=None, replace_properties=True
        )

    def test_delete(self, management_hook):
        management_hook.delete_catalog.return_value = False
        assert PolarisDeleteCatalogOperator(task_id="t", catalog_name="analytics").execute({}) is False
        management_hook.delete_catalog.assert_called_once_with("analytics", if_missing="skip")

    def test_template_fields(self):
        assert set(PolarisCreateCatalogOperator.template_fields) == {"polaris_conn_id", "catalog"}
        assert "storage_config_info" in PolarisUpdateCatalogOperator.template_fields


def test_operators_serialize():
    with DAG("polaris_catalog_operators", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        PolarisCreateCatalogOperator(task_id="create", catalog={**CATALOG, "name": "{{ ds_nodash }}"})
        PolarisUpdateCatalogOperator(task_id="update", catalog_name="c", properties={"a": "1"})
        PolarisDeleteCatalogOperator(task_id="delete", catalog_name="c")
    restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    assert sorted(restored.task_ids) == sorted(dag.task_ids)
    assert restored.get_task("create").catalog["name"] == "{{ ds_nodash }}"  # still a template
