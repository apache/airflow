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

import pytest

from airflow.providers.apache.polaris.operators.table import (
    PolarisCreateTableOperator,
    PolarisDropTableOperator,
    PolarisRegisterTableOperator,
    PolarisRenameTableOperator,
    PolarisUpdateTablePropertiesOperator,
    table_to_dict,
)
from airflow.providers.common.compat.sdk import DAG, AirflowFailException

from tests_common.test_utils.compat import DagSerialization

SCHEMA = {"type": "struct", "fields": [{"id": 1, "name": "id", "type": "long", "required": False}]}
RESULT_BODY = {
    "metadata-location": "s3://lake/sales/orders/metadata/00001.metadata.json",
    "metadata": {"format-version": 2, "table-uuid": "uuid-1", "location": "s3://lake/sales/orders"},
    "config": {"s3.access-key-id": "AKIA-VENDED", "s3.secret-access-key": "SECRET-VENDED"},
    "storage-credentials": [{"prefix": "s3://lake", "config": {"s3.secret-access-key": "SECRET-VENDED"}}],
}
SUMMARY = {
    "namespace": ["sales"],
    "name": "orders",
    "table_uuid": "uuid-1",
    "location": "s3://lake/sales/orders",
    "metadata_location": "s3://lake/sales/orders/metadata/00001.metadata.json",
    "format_version": 2,
}


def test_table_to_dict_leaves_out_storage_config_and_credentials():
    summary = table_to_dict(RESULT_BODY, "sales", "orders")
    assert summary == SUMMARY
    assert "VENDED" not in json.dumps(summary)


class TestPolarisCreateTableOperator:
    def test_execute(self, catalog_hook):
        catalog_hook.create_table.return_value = RESULT_BODY
        partition_spec = {
            "fields": [{"source-id": 1, "field-id": 1000, "name": "b", "transform": "bucket[4]"}]
        }
        result = PolarisCreateTableOperator(
            task_id="t",
            namespace="sales",
            table="orders",
            schema=SCHEMA,
            partition_spec=partition_spec,
            properties={"owner": "x"},
            catalog="lake",
        ).execute({})
        catalog_hook.create_table.assert_called_once_with(
            "sales",
            "orders",
            SCHEMA,
            partition_spec=partition_spec,
            sort_order=None,
            location=None,
            properties={"owner": "x"},
            catalog="lake",
            if_exists="skip",
        )
        assert result == SUMMARY

    def test_end_to_end_returns_no_vended_credentials(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", "/api/catalog/v1/lake/namespaces/sales/tables", (200, RESULT_BODY))
        result = PolarisCreateTableOperator(
            task_id="t",
            namespace="sales",
            table="orders",
            schema=SCHEMA,
            polaris_conn_id=polaris_conn(default_catalog="lake"),
        ).execute({})
        assert result == SUMMARY
        assert "VENDED" not in json.dumps(result)


class TestPolarisDropTableOperator:
    @pytest.mark.parametrize("purge", [False, True])
    def test_execute(self, catalog_hook, purge):
        catalog_hook.drop_table.return_value = True
        kwargs = {"purge": True} if purge else {}
        assert PolarisDropTableOperator(task_id="t", namespace="sales", table="orders", **kwargs).execute({})
        catalog_hook.drop_table.assert_called_once_with(
            "sales", "orders", purge=purge, catalog=None, if_missing="skip"
        )


class TestPolarisUpdateTablePropertiesOperator:
    def test_execute(self, catalog_hook):
        catalog_hook.update_table_properties.return_value = {
            "updated": ["a"],
            "removed": [],
            "properties": {"a": "1"},
        }
        result = PolarisUpdateTablePropertiesOperator(
            task_id="t", namespace="sales", table="orders", updates={"a": "1"}
        ).execute({})
        catalog_hook.update_table_properties.assert_called_once_with(
            "sales", "orders", updates={"a": "1"}, removals=None, catalog=None
        )
        assert result["updated"] == ["a"]

    def test_needs_updates_or_removals(self):
        with pytest.raises(ValueError, match="updates and/or removals"):
            PolarisUpdateTablePropertiesOperator(task_id="t", namespace="sales", table="orders")


class TestPolarisRegisterTableOperator:
    def test_execute(self, catalog_hook):
        catalog_hook.register_table.return_value = RESULT_BODY
        result = PolarisRegisterTableOperator(
            task_id="t", namespace="sales", table="orders", metadata_location="s3://lake/m.json"
        ).execute({})
        catalog_hook.register_table.assert_called_once_with(
            "sales", "orders", "s3://lake/m.json", catalog=None, if_exists="skip"
        )
        assert result == SUMMARY


class TestPolarisRenameTableOperator:
    def test_execute(self, catalog_hook):
        catalog_hook.rename_table.return_value = False
        result = PolarisRenameTableOperator(
            task_id="t", namespace="sales", table="orders", new_table="orders_v2", new_namespace="archive"
        ).execute({})
        assert result is False
        catalog_hook.rename_table.assert_called_once_with(
            "sales", "orders", "orders_v2", new_namespace="archive", catalog=None, if_missing="skip"
        )


def test_operators_serialize():
    with DAG("polaris_table_operators", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        PolarisCreateTableOperator(
            task_id="create",
            namespace="sales",
            table="{{ params.table }}",
            schema=SCHEMA,
            sort_order={"fields": []},
        )
        PolarisDropTableOperator(task_id="drop", namespace="sales", table="orders", purge=True)
        PolarisUpdateTablePropertiesOperator(
            task_id="update", namespace="sales", table="orders", removals=["a"]
        )
        PolarisRegisterTableOperator(
            task_id="register", namespace="sales", table="t", metadata_location="s3://m"
        )
        PolarisRenameTableOperator(task_id="rename", namespace="sales", table="t", new_table="u")
    restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    assert sorted(restored.task_ids) == sorted(dag.task_ids)
    assert restored.get_task("create").table == "{{ params.table }}"
    assert restored.get_task("create").schema == SCHEMA


def test_invalid_properties_fail_without_retry(fake_polaris, polaris_conn):
    op = PolarisUpdateTablePropertiesOperator(
        task_id="t",
        namespace="sales",
        table="orders",
        updates={"write.target-file-size-bytes": 536870912},
        polaris_conn_id=polaris_conn(default_catalog="lake"),
    )
    with pytest.raises(AirflowFailException, match="values must be strings"):
        op.execute({})
    assert not fake_polaris.calls("GET", "/api/catalog/v1/lake/namespaces/sales/tables/orders")
