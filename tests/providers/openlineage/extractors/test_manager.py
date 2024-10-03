#
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

import tempfile
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from openlineage.client.event_v2 import Dataset as OpenLineageDataset
from openlineage.client.facet_v2 import documentation_dataset, ownership_dataset, schema_dataset

from airflow.io.path import ObjectStoragePath
from airflow.lineage.entities import Column, File, Table, User
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.providers.openlineage.extractors.manager import ExtractorManager
from airflow.providers.openlineage.utils.utils import Asset
from airflow.utils.state import State
from tests.test_utils.compat import AIRFLOW_V_2_10_PLUS

if TYPE_CHECKING:
    from airflow.utils.context import Context

if AIRFLOW_V_2_10_PLUS:

    @pytest.fixture
    def hook_lineage_collector():
        from importlib.util import find_spec

        from airflow.lineage import hook

        if find_spec("airflow.assets"):
            # Dataset has been renamed as Asset in 3.0
            from airflow.lineage.hook import get_hook_lineage_collector
        else:
            from airflow.providers.openlineage.utils.asset_compat_lineage_collector import (
                get_hook_lineage_collector,
            )

        hook._hook_lineage_collector = None
        hook._hook_lineage_collector = hook.HookLineageCollector()

        yield get_hook_lineage_collector()

        hook._hook_lineage_collector = None


@pytest.mark.parametrize(
    ("uri", "dataset"),
    (
        ("s3://bucket1/dir1/file1", OpenLineageDataset(namespace="s3://bucket1", name="dir1/file1")),
        ("gs://bucket2/dir2/file2", OpenLineageDataset(namespace="gs://bucket2", name="dir2/file2")),
        ("gcs://bucket3/dir3/file3", OpenLineageDataset(namespace="gs://bucket3", name="dir3/file3")),
        (
            "hdfs://namenodehost:8020/file1",
            OpenLineageDataset(namespace="hdfs://namenodehost:8020", name="file1"),
        ),
        ("hdfs://namenodehost/file2", OpenLineageDataset(namespace="hdfs://namenodehost", name="file2")),
        ("file://localhost/etc/fstab", OpenLineageDataset(namespace="file://localhost", name="etc/fstab")),
        ("file:///etc/fstab", OpenLineageDataset(namespace="file://", name="etc/fstab")),
        ("https://test.com", OpenLineageDataset(namespace="https", name="test.com")),
        (
            "https://test.com?param1=test1&param2=test2",
            OpenLineageDataset(namespace="https", name="test.com"),
        ),
        ("file:test.csv", None),
        ("not_an_url", None),
    ),
)
def test_convert_to_ol_dataset_from_object_storage_uri(uri, dataset):
    result = ExtractorManager.convert_to_ol_dataset_from_object_storage_uri(uri)
    assert result == dataset


@pytest.mark.parametrize(
    ("obj", "dataset"),
    (
        (
            OpenLineageDataset(namespace="n1", name="f1"),
            OpenLineageDataset(namespace="n1", name="f1"),
        ),
        (
            File(url="s3://bucket1/dir1/file1"),
            OpenLineageDataset(namespace="s3://bucket1", name="dir1/file1"),
        ),
        (
            File(url="gs://bucket2/dir2/file2"),
            OpenLineageDataset(namespace="gs://bucket2", name="dir2/file2"),
        ),
        (
            File(url="gcs://bucket3/dir3/file3"),
            OpenLineageDataset(namespace="gs://bucket3", name="dir3/file3"),
        ),
        (
            File(url="hdfs://namenodehost:8020/file1"),
            OpenLineageDataset(namespace="hdfs://namenodehost:8020", name="file1"),
        ),
        (
            File(url="hdfs://namenodehost/file2"),
            OpenLineageDataset(namespace="hdfs://namenodehost", name="file2"),
        ),
        (
            File(url="file://localhost/etc/fstab"),
            OpenLineageDataset(namespace="file://localhost", name="etc/fstab"),
        ),
        (File(url="file:///etc/fstab"), OpenLineageDataset(namespace="file://", name="etc/fstab")),
        (File(url="https://test.com"), OpenLineageDataset(namespace="https", name="test.com")),
        (Table(cluster="c1", database="d1", name="t1"), OpenLineageDataset(namespace="c1", name="d1.t1")),
        ("gs://bucket2/dir2/file2", None),
        ("not_an_url", None),
    ),
)
def test_convert_to_ol_dataset(obj, dataset):
    result = ExtractorManager.convert_to_ol_dataset(obj)
    assert result == dataset


def test_convert_to_ol_dataset_from_table_with_columns_and_owners():
    table = Table(
        cluster="c1",
        database="d1",
        name="t1",
        columns=[
            Column(name="col1", description="desc1", data_type="type1"),
            Column(name="col2", description="desc2", data_type="type2"),
        ],
        owners=[
            User(email="mike@company.com", first_name="Mike", last_name="Smith"),
            User(email="theo@company.com", first_name="Theo"),
            User(email="smith@company.com", last_name="Smith"),
            User(email="jane@company.com"),
        ],
        description="test description",
    )
    expected_facets = {
        "schema": schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(
                    name="col1",
                    type="type1",
                    description="desc1",
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="col2",
                    type="type2",
                    description="desc2",
                ),
            ]
        ),
        "ownership": ownership_dataset.OwnershipDatasetFacet(
            owners=[
                ownership_dataset.Owner(name="user:Mike Smith <mike@company.com>", type=""),
                ownership_dataset.Owner(name="user:Theo <theo@company.com>", type=""),
                ownership_dataset.Owner(name="user:Smith <smith@company.com>", type=""),
                ownership_dataset.Owner(name="user:<jane@company.com>", type=""),
            ]
        ),
        "documentation": documentation_dataset.DocumentationDatasetFacet(description="test description"),
    }
    result = ExtractorManager.convert_to_ol_dataset_from_table(table)
    assert result.namespace == "c1"
    assert result.name == "d1.t1"
    assert result.facets == expected_facets


def test_convert_to_ol_dataset_table():
    table = Table(
        cluster="c1",
        database="d1",
        name="t1",
        columns=[
            Column(name="col1", description="desc1", data_type="type1"),
            Column(name="col2", description="desc2", data_type="type2"),
        ],
        owners=[
            User(email="mike@company.com", first_name="Mike", last_name="Smith"),
            User(email="theo@company.com", first_name="Theo"),
            User(email="smith@company.com", last_name="Smith"),
            User(email="jane@company.com"),
        ],
    )
    expected_facets = {
        "schema": schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(
                    name="col1",
                    type="type1",
                    description="desc1",
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="col2",
                    type="type2",
                    description="desc2",
                ),
            ]
        ),
        "ownership": ownership_dataset.OwnershipDatasetFacet(
            owners=[
                ownership_dataset.Owner(name="user:Mike Smith <mike@company.com>", type=""),
                ownership_dataset.Owner(name="user:Theo <theo@company.com>", type=""),
                ownership_dataset.Owner(name="user:Smith <smith@company.com>", type=""),
                ownership_dataset.Owner(name="user:<jane@company.com>", type=""),
            ]
        ),
    }

    result = ExtractorManager.convert_to_ol_dataset(table)
    assert result.namespace == "c1"
    assert result.name == "d1.t1"
    assert result.facets == expected_facets


@pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Hook lineage works in Airflow >= 2.10.0")
def test_extractor_manager_uses_hook_level_lineage(hook_lineage_collector):
    dagrun = MagicMock()
    task = MagicMock()
    del task.get_openlineage_facets_on_start
    del task.get_openlineage_facets_on_complete
    ti = MagicMock()

    hook_lineage_collector.add_input_asset(None, uri="s3://bucket/input_key")
    hook_lineage_collector.add_output_asset(None, uri="s3://bucket/output_key")
    extractor_manager = ExtractorManager()
    metadata = extractor_manager.extract_metadata(dagrun=dagrun, task=task, complete=True, task_instance=ti)

    assert metadata.inputs == [OpenLineageDataset(namespace="s3://bucket", name="input_key")]
    assert metadata.outputs == [OpenLineageDataset(namespace="s3://bucket", name="output_key")]


@pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Hook lineage works in Airflow >= 2.10.0")
def test_extractor_manager_does_not_use_hook_level_lineage_when_operator(hook_lineage_collector):
    class FakeSupportedOperator(BaseOperator):
        def execute(self, context: Context) -> Any:
            pass

        def get_openlineage_facets_on_start(self):
            return OperatorLineage(
                inputs=[OpenLineageDataset(namespace="s3://bucket", name="proper_input_key")]
            )

    dagrun = MagicMock()
    task = FakeSupportedOperator(task_id="test_task_extractor")
    ti = MagicMock()
    hook_lineage_collector.add_input_asset(None, uri="s3://bucket/input_key")

    extractor_manager = ExtractorManager()
    metadata = extractor_manager.extract_metadata(dagrun=dagrun, task=task, complete=True, task_instance=ti)

    # s3://bucket/input_key not here - use data from operator
    assert metadata.inputs == [OpenLineageDataset(namespace="s3://bucket", name="proper_input_key")]
    assert metadata.outputs == []


@pytest.mark.db_test
@pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Hook lineage works in Airflow >= 2.10.0")
def test_extractor_manager_gets_data_from_pythonoperator(session, dag_maker, hook_lineage_collector):
    path = None
    with tempfile.NamedTemporaryFile() as f:
        path = f.name
        with dag_maker():

            def use_read():
                storage_path = ObjectStoragePath(path)
                with storage_path.open("w") as out:
                    out.write("test")

            task = PythonOperator(task_id="test_task_extractor_pythonoperator", python_callable=use_read)

    dr = dag_maker.create_dagrun()
    ti = TaskInstance(task=task, run_id=dr.run_id)
    ti.state = State.QUEUED
    session.merge(ti)
    session.commit()

    ti.run()

    datasets = hook_lineage_collector.collected_assets

    assert len(datasets.outputs) == 1
    assert datasets.outputs[0].asset == Asset(uri=path)
