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

from datetime import datetime

import pytest

from airflow.providers.apache.polaris.operators.namespace import (
    PolarisCreateNamespaceOperator,
    PolarisDropNamespaceOperator,
    PolarisUpdateNamespacePropertiesOperator,
)
from airflow.providers.common.compat.sdk import DAG, AirflowFailException

from tests_common.test_utils.compat import DagSerialization


class TestPolarisCreateNamespaceOperator:
    def test_execute(self, catalog_hook):
        catalog_hook.create_namespace.return_value = {"owner": "x", "location": "s3://l"}
        result = PolarisCreateNamespaceOperator(
            task_id="t", namespace="sales.eu", properties={"owner": "x"}, catalog="lake"
        ).execute({})
        catalog_hook.create_namespace.assert_called_once_with(
            "sales.eu", properties={"owner": "x"}, catalog="lake", if_exists="skip"
        )
        assert result == {"namespace": ["sales", "eu"], "properties": {"owner": "x", "location": "s3://l"}}


class TestPolarisUpdateNamespacePropertiesOperator:
    def test_execute(self, catalog_hook):
        catalog_hook.update_namespace_properties.return_value = {
            "updated": ["a"],
            "removed": ["b"],
            "missing": ["c"],
        }
        result = PolarisUpdateNamespacePropertiesOperator(
            task_id="t", namespace=["sales"], updates={"a": "1"}, removals=["b", "c"]
        ).execute({})
        catalog_hook.update_namespace_properties.assert_called_once_with(
            ["sales"], updates={"a": "1"}, removals=["b", "c"], catalog=None
        )
        assert result == {"updated": ["a"], "removed": ["b"], "missing": ["c"]}

    def test_needs_updates_or_removals(self):
        with pytest.raises(ValueError, match="updates and/or removals"):
            PolarisUpdateNamespacePropertiesOperator(task_id="t", namespace="sales")


class TestPolarisDropNamespaceOperator:
    def test_execute_defaults_to_skip(self, catalog_hook):
        catalog_hook.drop_namespace.return_value = False
        assert PolarisDropNamespaceOperator(task_id="t", namespace="sales").execute({}) is False
        catalog_hook.drop_namespace.assert_called_once_with("sales", catalog=None, if_missing="skip")

    def test_namespace_with_contents_fails_without_retry(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "DELETE",
            "/api/catalog/v1/lake/namespaces/sales",
            (
                409,
                {
                    "error": {
                        "message": "Namespace sales is not empty",
                        "type": "NamespaceNotEmptyException",
                        "code": 409,
                    }
                },
            ),
        )
        op = PolarisDropNamespaceOperator(
            task_id="t", namespace="sales", polaris_conn_id=polaris_conn(default_catalog="lake")
        )
        with pytest.raises(AirflowFailException, match="not empty"):
            op.execute({})


def test_operators_serialize():
    with DAG("polaris_namespace_operators", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        PolarisCreateNamespaceOperator(
            task_id="create", namespace="{{ params.ns }}", properties={"owner": "x"}
        )
        PolarisUpdateNamespacePropertiesOperator(task_id="update", namespace="sales", removals=["a"])
        PolarisDropNamespaceOperator(task_id="drop", namespace="sales")
    restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    assert sorted(restored.task_ids) == sorted(dag.task_ids)
    assert restored.get_task("create").namespace == "{{ params.ns }}"
