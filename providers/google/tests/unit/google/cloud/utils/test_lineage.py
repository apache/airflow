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

from unittest import mock

from google.cloud.bigquery import CopyJob, DatasetReference, ExtractJob, LoadJob, QueryJob, TableReference

from airflow.providers.common.compat.assets import Asset
from airflow.providers.google.cloud.utils.lineage import (
    _add_bq_table_to_lineage,
    _add_gcs_uris_to_lineage,
    send_hook_lineage_for_bq_job,
)

PROJECT_ID = "test-project"
DATASET_ID = "test_dataset"
TABLE_ID = "test_table"
JOB_ID = "test-job-123"

TABLE_REFERENCE = TableReference(DatasetReference(PROJECT_ID, DATASET_ID), TABLE_ID)


def _make_table_ref(project, dataset, table):
    return TableReference(DatasetReference(project, dataset), table)


class TestAddBqTableToLineage:
    def test_add_as_input(self):
        collector = mock.MagicMock()
        context = mock.sentinel.context

        _add_bq_table_to_lineage(collector, context, TABLE_REFERENCE, is_input=True)

        collector.add_input_asset.assert_called_once_with(
            context=context,
            scheme="bigquery",
            asset_kwargs={
                "project_id": PROJECT_ID,
                "dataset_id": DATASET_ID,
                "table_id": TABLE_ID,
            },
        )
        collector.add_output_asset.assert_not_called()

    def test_add_as_output(self):
        collector = mock.MagicMock()
        context = mock.sentinel.context

        _add_bq_table_to_lineage(collector, context, TABLE_REFERENCE, is_input=False)

        collector.add_output_asset.assert_called_once_with(
            context=context,
            scheme="bigquery",
            asset_kwargs={
                "project_id": PROJECT_ID,
                "dataset_id": DATASET_ID,
                "table_id": TABLE_ID,
            },
        )
        collector.add_input_asset.assert_not_called()


class TestAddGcsUrisToLineage:
    def test_add_uris_as_input(self):
        collector = mock.MagicMock()
        context = mock.sentinel.context
        uris = ["gs://bucket1/path/file.csv", "gs://bucket2/other.json"]

        _add_gcs_uris_to_lineage(collector, context, uris, is_input=True)

        assert collector.add_input_asset.call_count == 2
        collector.add_input_asset.assert_any_call(context=context, uri="gs://bucket1/path/file.csv")
        collector.add_input_asset.assert_any_call(context=context, uri="gs://bucket2/other.json")
        collector.add_output_asset.assert_not_called()

    def test_add_uris_as_output(self):
        collector = mock.MagicMock()
        context = mock.sentinel.context
        uris = ["gs://bucket/export/data.csv"]

        _add_gcs_uris_to_lineage(collector, context, uris, is_input=False)

        collector.add_output_asset.assert_called_once_with(context=context, uri="gs://bucket/export/data.csv")
        collector.add_input_asset.assert_not_called()

    def test_empty_uris(self):
        collector = mock.MagicMock()
        _add_gcs_uris_to_lineage(collector, mock.sentinel.context, [], is_input=True)
        collector.add_input_asset.assert_not_called()

    def test_none_uris(self):
        collector = mock.MagicMock()
        _add_gcs_uris_to_lineage(collector, mock.sentinel.context, None, is_input=True)
        collector.add_input_asset.assert_not_called()


class TestSendHookLineageForBqJob:
    @mock.patch("airflow.providers.google.cloud.utils.lineage.send_sql_hook_lineage")
    def test_query_job(self, mock_send_sql):
        job = mock.MagicMock(spec=QueryJob)
        job.query = "SELECT * FROM dataset.table"
        job.job_id = JOB_ID
        job.default_dataset = DatasetReference(PROJECT_ID, DATASET_ID)
        context = mock.sentinel.context

        send_hook_lineage_for_bq_job(context=context, job=job)

        mock_send_sql.assert_called_once_with(
            context=context,
            sql="SELECT * FROM dataset.table",
            job_id=JOB_ID,
            default_db=PROJECT_ID,
            default_schema=DATASET_ID,
        )

    @mock.patch("airflow.providers.google.cloud.utils.lineage.send_sql_hook_lineage")
    def test_query_job_no_default_dataset(self, mock_send_sql):
        job = mock.MagicMock(spec=QueryJob)
        job.query = "SELECT 1"
        job.job_id = JOB_ID
        job.default_dataset = None
        context = mock.sentinel.context

        send_hook_lineage_for_bq_job(context=context, job=job)

        mock_send_sql.assert_called_once_with(
            context=context,
            sql="SELECT 1",
            job_id=JOB_ID,
            default_db=None,
            default_schema=None,
        )

    def test_load_job(self, hook_lineage_collector):
        job = mock.MagicMock(spec=LoadJob)
        job.source_uris = ["gs://bucket/data.csv", "gs://bucket/data2.csv"]
        job.destination = TABLE_REFERENCE
        context = mock.sentinel.context

        send_hook_lineage_for_bq_job(context=context, job=job)

        assert len(hook_lineage_collector.collected_assets.inputs) == 2
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"bigquery://{PROJECT_ID}/{DATASET_ID}/{TABLE_ID}"
        )

    def test_load_job_no_destination(self, hook_lineage_collector):
        job = mock.MagicMock(spec=LoadJob)
        job.source_uris = ["gs://bucket/data.csv"]
        job.destination = None
        context = mock.sentinel.context

        send_hook_lineage_for_bq_job(context=context, job=job)

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert len(hook_lineage_collector.collected_assets.outputs) == 0

    def test_copy_job(self, hook_lineage_collector):
        source1 = _make_table_ref(PROJECT_ID, DATASET_ID, "source1")
        source2 = _make_table_ref(PROJECT_ID, DATASET_ID, "source2")
        dest = _make_table_ref(PROJECT_ID, DATASET_ID, "dest")

        job = mock.MagicMock(spec=CopyJob)
        job.sources = [source1, source2]
        job.destination = dest
        context = mock.sentinel.context

        send_hook_lineage_for_bq_job(context=context, job=job)

        assert len(hook_lineage_collector.collected_assets.inputs) == 2
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri=f"bigquery://{PROJECT_ID}/{DATASET_ID}/source1"
        )
        assert hook_lineage_collector.collected_assets.inputs[1].asset == Asset(
            uri=f"bigquery://{PROJECT_ID}/{DATASET_ID}/source2"
        )
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"bigquery://{PROJECT_ID}/{DATASET_ID}/dest"
        )

    def test_extract_job(self, hook_lineage_collector):
        job = mock.MagicMock(spec=ExtractJob)
        job.source = TABLE_REFERENCE
        job.destination_uris = ["gs://bucket/export/file1.csv", "gs://bucket/export/file2.csv"]
        context = mock.sentinel.context

        send_hook_lineage_for_bq_job(context=context, job=job)

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert len(hook_lineage_collector.collected_assets.outputs) == 2
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri=f"bigquery://{PROJECT_ID}/{DATASET_ID}/{TABLE_ID}"
        )

    def test_extract_job_no_source(self, hook_lineage_collector):
        job = mock.MagicMock(spec=ExtractJob)
        job.source = None
        job.destination_uris = ["gs://bucket/export/file.csv"]
        context = mock.sentinel.context

        send_hook_lineage_for_bq_job(context=context, job=job)

        assert len(hook_lineage_collector.collected_assets.inputs) == 0
        assert len(hook_lineage_collector.collected_assets.outputs) == 1

    @mock.patch("airflow.providers.google.cloud.utils.lineage.send_sql_hook_lineage")
    def test_unknown_job_type_does_not_raise(self, mock_send_sql, hook_lineage_collector):
        job = mock.MagicMock()
        send_hook_lineage_for_bq_job(context=mock.sentinel.context, job=job)
        mock_send_sql.assert_not_called()
        assert len(hook_lineage_collector.collected_assets.inputs) == 0
        assert len(hook_lineage_collector.collected_assets.outputs) == 0

    def test_exception_in_non_query_job_is_caught(self, hook_lineage_collector):
        job = mock.MagicMock(spec=LoadJob)
        type(job).source_uris = mock.PropertyMock(side_effect=RuntimeError("boom"))
        context = mock.sentinel.context

        send_hook_lineage_for_bq_job(context=context, job=job)
