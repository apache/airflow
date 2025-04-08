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

import logging
import tempfile
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol
from unittest import mock
from unittest.mock import MagicMock

import pytest
from openlineage.client.event_v2 import Dataset as OpenLineageDataset
from openlineage.client.facet_v2 import (
    documentation_dataset,
    ownership_dataset,
    schema_dataset,
)
from uuid6 import uuid7

from airflow.io.path import ObjectStoragePath
from airflow.models.taskinstance import TaskInstance
from airflow.providers.common.compat.lineage.entities import Column, File, Table, User
from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.providers.openlineage.extractors.manager import ExtractorManager
from airflow.providers.openlineage.utils.utils import Asset
from airflow.utils.state import State

from tests_common.test_utils.compat import DateTimeSensor, PythonOperator
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker
from tests_common.test_utils.version_compat import AIRFLOW_V_2_10_PLUS, AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from collections.abc import Sequence

    try:
        from airflow.sdk.api.datamodels._generated import AssetEventDagRunReference, TIRunContext
        from airflow.sdk.definitions.context import Context

    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        # TIRunContext is only used in Airflow 3 tests
        from airflow.utils.context import Context

        AssetEventDagRunReference = TIRunContext = Any  # type: ignore[misc, assignment]


if AIRFLOW_V_2_10_PLUS:

    @pytest.fixture
    def hook_lineage_collector():
        from airflow.lineage import hook
        from airflow.providers.common.compat.lineage.hook import (
            get_hook_lineage_collector,
        )

        hook._hook_lineage_collector = None
        hook._hook_lineage_collector = hook.HookLineageCollector()

        yield get_hook_lineage_collector()

        hook._hook_lineage_collector = None


if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance as SDKTaskInstance
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.execution_time import task_runner
    from airflow.sdk.execution_time.comms import StartupDetails
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, parse
else:
    from airflow.models.baseoperator import BaseOperator

    SDKTaskInstance = ...  # type: ignore
    task_runner = ...  # type: ignore
    StartupDetails = ...  # type: ignore
    RuntimeTaskInstance = ...  # type: ignore
    parse = ...  # type: ignore


@pytest.mark.parametrize(
    ("uri", "dataset"),
    (
        (
            "s3://bucket1/dir1/file1",
            OpenLineageDataset(namespace="s3://bucket1", name="dir1/file1"),
        ),
        (
            "gs://bucket2/dir2/file2",
            OpenLineageDataset(namespace="gs://bucket2", name="dir2/file2"),
        ),
        (
            "gcs://bucket3/dir3/file3",
            OpenLineageDataset(namespace="gs://bucket3", name="dir3/file3"),
        ),
        (
            "hdfs://namenodehost:8020/file1",
            OpenLineageDataset(namespace="hdfs://namenodehost:8020", name="file1"),
        ),
        (
            "hdfs://namenodehost/file2",
            OpenLineageDataset(namespace="hdfs://namenodehost", name="file2"),
        ),
        (
            "file://localhost/etc/fstab",
            OpenLineageDataset(namespace="file://localhost", name="etc/fstab"),
        ),
        (
            "file:///etc/fstab",
            OpenLineageDataset(namespace="file://", name="etc/fstab"),
        ),
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
        (
            File(url="file:///etc/fstab"),
            OpenLineageDataset(namespace="file://", name="etc/fstab"),
        ),
        (
            File(url="https://test.com"),
            OpenLineageDataset(namespace="https", name="test.com"),
        ),
        (
            Table(cluster="c1", database="d1", name="t1"),
            OpenLineageDataset(namespace="c1", name="d1.t1"),
        ),
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


@skip_if_force_lowest_dependencies_marker
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
    metadata = extractor_manager.extract_metadata(
        dagrun=dagrun, task=task, task_instance_state=None, task_instance=ti
    )

    assert metadata.inputs == [OpenLineageDataset(namespace="s3://bucket", name="input_key")]
    assert metadata.outputs == [OpenLineageDataset(namespace="s3://bucket", name="output_key")]


@pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Hook lineage works in Airflow >= 2.10.0")
def test_extractor_manager_does_not_use_hook_level_lineage_when_operator(
    hook_lineage_collector,
):
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
    metadata = extractor_manager.extract_metadata(
        dagrun=dagrun, task=task, task_instance_state=None, task_instance=ti
    )

    # s3://bucket/input_key not here - use data from operator
    assert metadata.inputs == [OpenLineageDataset(namespace="s3://bucket", name="proper_input_key")]
    assert metadata.outputs == []


@pytest.mark.db_test
@pytest.mark.skipif(
    not AIRFLOW_V_2_10_PLUS or AIRFLOW_V_3_0_PLUS,
    reason="Test for hook level lineage in Airflow >= 2.10.0 < 3.0",
)
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
    ti.refresh_from_db()
    ti.state = State.QUEUED
    session.merge(ti)
    session.commit()

    ti.run()

    datasets = hook_lineage_collector.collected_assets

    assert len(datasets.outputs) == 1
    assert datasets.outputs[0].asset == Asset(uri=path)


@pytest.fixture
def mock_supervisor_comms():
    with mock.patch(
        "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
    ) as supervisor_comms:
        yield supervisor_comms


@pytest.fixture
def mocked_parse(spy_agency):
    """
    Fixture to set up an inline DAG and use it in a stubbed `parse` function. Use this fixture if you
    want to isolate and test `parse` or `run` logic without having to define a DAG file.

    This fixture returns a helper function `set_dag` that:
    1. Creates an in line DAG with the given `dag_id` and `task` (limited to one task)
    2. Constructs a `RuntimeTaskInstance` based on the provided `StartupDetails` and task.
    3. Stubs the `parse` function using `spy_agency`, to return the mocked `RuntimeTaskInstance`.

    After adding the fixture in your test function signature, you can use it like this ::

            mocked_parse(
                StartupDetails(
                    ti=TaskInstance(
                        id=uuid7(), task_id="hello", dag_id="super_basic_run", run_id="c", try_number=1
                    ),
                    file="",
                    requests_fd=0,
                ),
                "example_dag_id",
                CustomOperator(task_id="hello"),
            )
    """

    def set_dag(what: StartupDetails, dag_id: str, task: BaseOperator) -> RuntimeTaskInstance:
        from airflow.sdk.definitions.dag import DAG
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, parse
        from airflow.utils import timezone

        if not task.has_dag():
            dag = DAG(dag_id=dag_id, start_date=timezone.datetime(2024, 12, 3))
            task.dag = dag
            task = dag.task_dict[task.task_id]
        else:
            dag = task.dag
        if what.ti_context.dag_run.conf:
            dag.params = what.ti_context.dag_run.conf  # type: ignore[assignment]
        ti = RuntimeTaskInstance.model_construct(
            **what.ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=what.ti_context,
            max_tries=what.ti_context.max_tries,
            start_date=what.start_date,
        )
        if hasattr(parse, "spy"):
            spy_agency.unspy(parse)
        spy_agency.spy_on(parse, call_fake=lambda _: ti)
        return ti

    return set_dag


class MakeTIContextCallable(Protocol):
    def __call__(
        self,
        dag_id: str = ...,
        run_id: str = ...,
        logical_date: str | datetime = ...,
        data_interval_start: str | datetime = ...,
        data_interval_end: str | datetime = ...,
        clear_number: int = ...,
        start_date: str | datetime = ...,
        run_after: str | datetime = ...,
        run_type: str = ...,
        task_reschedule_count: int = ...,
        conf: dict[str, Any] | None = ...,
        consumed_asset_events: Sequence[AssetEventDagRunReference] = ...,
    ) -> TIRunContext: ...


# Only needed in Airflow 3
@pytest.fixture
def make_ti_context() -> MakeTIContextCallable:
    """Factory for creating TIRunContext objects."""
    from airflow.sdk.api.datamodels._generated import DagRun, TIRunContext

    def _make_context(
        dag_id: str = "test_dag",
        run_id: str = "test_run",
        logical_date: str | datetime = "2024-12-01T01:00:00Z",
        data_interval_start: str | datetime = "2024-12-01T00:00:00Z",
        data_interval_end: str | datetime = "2024-12-01T01:00:00Z",
        clear_number: int = 0,
        start_date: str | datetime = "2024-12-01T01:00:00Z",
        run_after: str | datetime = "2024-12-01T01:00:00Z",
        run_type: str = "manual",
        task_reschedule_count: int = 0,
        conf=None,
        consumed_asset_events: Sequence[AssetEventDagRunReference] = (),
    ) -> TIRunContext:
        return TIRunContext(
            dag_run=DagRun(
                dag_id=dag_id,
                run_id=run_id,
                logical_date=logical_date,  # type: ignore
                data_interval_start=data_interval_start,  # type: ignore
                data_interval_end=data_interval_end,  # type: ignore
                clear_number=clear_number,  # type: ignore
                start_date=start_date,  # type: ignore
                run_type=run_type,  # type: ignore
                run_after=run_after,  # type: ignore
                conf=conf,  # type: ignore
                consumed_asset_events=list(consumed_asset_events),
            ),
            task_reschedule_count=task_reschedule_count,
            max_tries=0,
            should_retry=False,
        )

    return _make_context


@pytest.mark.db_test
@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Task SDK related test")
def test_extractor_manager_gets_data_from_pythonoperator_tasksdk(
    session, hook_lineage_collector, mocked_parse, make_ti_context, mock_supervisor_comms
):
    path = None
    with tempfile.NamedTemporaryFile() as f:
        path = f.name

        def use_read():
            storage_path = ObjectStoragePath(path)
            with storage_path.open("w") as out:
                out.write("test")

    task = PythonOperator(task_id="test_task_extractor_pythonoperator", python_callable=use_read)
    FAKE_BUNDLE = BundleInfo.model_construct(name="anything", version="any")

    what = StartupDetails(
        ti=SDKTaskInstance(
            id=uuid7(),
            task_id="test_task_extractor_pythonoperator",
            dag_id="test_hookcollector_dag",
            run_id="c",
            try_number=1,
        ),
        dag_rel_path="",
        bundle_info=FAKE_BUNDLE,
        requests_fd=0,
        ti_context=make_ti_context(),
        start_date=datetime.now(),
    )
    ti = mocked_parse(what, "test_hookcollector_dag", task)

    task_runner.run(ti, ti.get_template_context(), logging.getLogger(__name__))

    datasets = hook_lineage_collector.collected_assets

    assert len(datasets.outputs) == 1
    assert datasets.outputs[0].asset == Asset(uri=path)


def test_extract_inlets_and_outlets_with_operator():
    inlets = [OpenLineageDataset(namespace="namespace1", name="name1")]
    outlets = [OpenLineageDataset(namespace="namespace2", name="name2")]

    extractor_manager = ExtractorManager()
    task = PythonOperator(task_id="task_id", python_callable=lambda x: x, inlets=inlets, outlets=outlets)
    lineage = OperatorLineage()
    extractor_manager.extract_inlets_and_outlets(lineage, task)
    assert lineage.inputs == inlets
    assert lineage.outputs == outlets


def test_extract_inlets_and_outlets_with_sensor():
    inlets = [OpenLineageDataset(namespace="namespace1", name="name1")]
    outlets = [OpenLineageDataset(namespace="namespace2", name="name2")]

    extractor_manager = ExtractorManager()
    task = DateTimeSensor(
        task_id="task_id", target_time="2025-04-04T08:48:13.713922+00:00", inlets=inlets, outlets=outlets
    )
    lineage = OperatorLineage()
    extractor_manager.extract_inlets_and_outlets(lineage, task)
    assert lineage.inputs == inlets
    assert lineage.outputs == outlets
