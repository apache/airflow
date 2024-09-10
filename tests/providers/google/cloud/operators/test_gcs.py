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

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pendulum
import pytest

from airflow.providers.common.compat.openlineage.facet import (
    Dataset,
    LifecycleStateChange,
    LifecycleStateChangeDatasetFacet,
    PreviousIdentifier,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSDeleteObjectsOperator,
    GCSFileTransformOperator,
    GCSListObjectsOperator,
    GCSObjectCreateAclEntryOperator,
    GCSSynchronizeBucketsOperator,
    GCSTimeSpanFileTransformOperator,
)
from airflow.timetables.base import DagRunInfo, DataInterval

TASK_ID = "test-gcs-operator"
TEST_BUCKET = "test-bucket"
TEST_PROJECT = "test-project"
DELIMITER = ".csv"
PREFIX = "TEST"
PREFIX_2 = "TEST2"
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv", "OTHERTEST1.csv"]
TEST_OBJECT = "dir1/test-object"
LOCAL_FILE_PATH = "/home/airflow/gcp/test-object"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestGoogleCloudStorageCreateBucket:
    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_execute(self, mock_hook):
        operator = GCSCreateBucketOperator(
            task_id=TASK_ID,
            bucket_name=TEST_BUCKET,
            resource={"lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7}}]}},
            storage_class="MULTI_REGIONAL",
            location="EU",
            labels={"env": "prod"},
            project_id=TEST_PROJECT,
        )

        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.create_bucket.assert_called_once_with(
            bucket_name=TEST_BUCKET,
            storage_class="MULTI_REGIONAL",
            location="EU",
            labels={"env": "prod"},
            project_id=TEST_PROJECT,
            resource={"lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 7}}]}},
        )


class TestGoogleCloudStorageAcl:
    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_bucket_create_acl(self, mock_hook):
        operator = GCSBucketCreateAclEntryOperator(
            bucket="test-bucket",
            entity="test-entity",
            role="test-role",
            user_project="test-user-project",
            task_id="id",
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.insert_bucket_acl.assert_called_once_with(
            bucket_name="test-bucket",
            entity="test-entity",
            role="test-role",
            user_project="test-user-project",
        )

    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_object_create_acl(self, mock_hook):
        operator = GCSObjectCreateAclEntryOperator(
            bucket="test-bucket",
            object_name="test-object",
            entity="test-entity",
            generation=42,
            role="test-role",
            user_project="test-user-project",
            task_id="id",
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.insert_object_acl.assert_called_once_with(
            bucket_name="test-bucket",
            object_name="test-object",
            entity="test-entity",
            generation=42,
            role="test-role",
            user_project="test-user-project",
        )


class TestGCSDeleteObjectsOperator:
    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_delete_objects(self, mock_hook):
        operator = GCSDeleteObjectsOperator(task_id=TASK_ID, bucket_name=TEST_BUCKET, objects=MOCK_FILES[0:2])

        operator.execute(None)
        mock_hook.return_value.list.assert_not_called()
        mock_hook.return_value.delete.assert_has_calls(
            calls=[
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[0]),
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[1]),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_delete_empty_list_of_objects(self, mock_hook):
        operator = GCSDeleteObjectsOperator(task_id=TASK_ID, bucket_name=TEST_BUCKET, objects=[])

        operator.execute(None)
        mock_hook.return_value.list.assert_not_called()
        mock_hook.return_value.delete.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_delete_prefix(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES[1:4]
        operator = GCSDeleteObjectsOperator(task_id=TASK_ID, bucket_name=TEST_BUCKET, prefix=PREFIX)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(bucket_name=TEST_BUCKET, prefix=PREFIX)
        mock_hook.return_value.delete.assert_has_calls(
            calls=[
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[1]),
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[2]),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_delete_prefix_as_empty_string(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES[0:4]
        operator = GCSDeleteObjectsOperator(task_id=TASK_ID, bucket_name=TEST_BUCKET, prefix="")

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(bucket_name=TEST_BUCKET, prefix="")
        mock_hook.return_value.delete.assert_has_calls(
            calls=[
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[0]),
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[1]),
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[2]),
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[3]),
            ],
            any_order=True,
        )

    @pytest.mark.parametrize(
        ("objects", "prefix", "inputs"),
        (
            (["folder/a.txt", "b.json"], None, ["folder/a.txt", "b.json"]),
            (["folder/a.txt", "folder/b.json"], None, ["folder/a.txt", "folder/b.json"]),
            (None, ["folder/a.txt", "b.json"], ["folder/a.txt", "b.json"]),
            (None, "dir/pre", ["dir"]),
            (None, ["dir/"], ["dir"]),
            (None, "", ["/"]),
            (None, "/", ["/"]),
            (None, "pre", ["/"]),
            (None, "dir/pre*", ["dir"]),
            (None, "*", ["/"]),
        ),
        ids=(
            "objects",
            "multiple objects in the same dir",
            "objects as prefixes",
            "directory with prefix",
            "directory",
            "empty prefix",
            "slash as prefix",
            "prefix with no ending slash",
            "directory with prefix with wildcard",
            "just wildcard",
        ),
    )
    def test_get_openlineage_facets_on_start(self, objects, prefix, inputs):
        bucket_url = f"gs://{TEST_BUCKET}"
        expected_inputs = [
            Dataset(
                namespace=bucket_url,
                name=name,
                facets={
                    "lifecycleStateChange": LifecycleStateChangeDatasetFacet(
                        lifecycleStateChange=LifecycleStateChange.DROP.value,
                        previousIdentifier=PreviousIdentifier(
                            namespace=bucket_url,
                            name=name,
                        ),
                    )
                },
            )
            for name in inputs
        ]

        operator = GCSDeleteObjectsOperator(
            task_id=TASK_ID, bucket_name=TEST_BUCKET, objects=objects, prefix=prefix
        )
        lineage = operator.get_openlineage_facets_on_start()
        assert len(lineage.inputs) == len(inputs)
        assert len(lineage.outputs) == 0
        assert all(element in lineage.inputs for element in expected_inputs)
        assert all(element in expected_inputs for element in lineage.inputs)


class TestGoogleCloudStorageListOperator:
    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_execute__delimiter(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        operator = GCSListObjectsOperator(
            task_id=TASK_ID, bucket=TEST_BUCKET, prefix=PREFIX, delimiter=DELIMITER
        )
        files = operator.execute(context=mock.MagicMock())
        mock_hook.return_value.list.assert_called_once_with(
            bucket_name=TEST_BUCKET, prefix=PREFIX, delimiter=DELIMITER, match_glob=None
        )
        assert sorted(files) == sorted(MOCK_FILES)

    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_execute__match_glob(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        operator = GCSListObjectsOperator(
            task_id=TASK_ID, bucket=TEST_BUCKET, prefix=PREFIX, match_glob=f"**/*{DELIMITER}", delimiter=None
        )
        files = operator.execute(context=mock.MagicMock())
        mock_hook.return_value.list.assert_called_once_with(
            bucket_name=TEST_BUCKET, prefix=PREFIX, match_glob=f"**/*{DELIMITER}", delimiter=None
        )
        assert sorted(files) == sorted(MOCK_FILES)


class TestGCSFileTransformOperator:
    @mock.patch("airflow.providers.google.cloud.operators.gcs.NamedTemporaryFile")
    @mock.patch("airflow.providers.google.cloud.operators.gcs.subprocess")
    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_execute(self, mock_hook, mock_subprocess, mock_tempfile):
        source_bucket = TEST_BUCKET
        source_object = "test.txt"
        destination_bucket = TEST_BUCKET + "-dest"
        destination_object = "transformed_test.txt"
        transform_script = "script.py"

        source = "source"
        destination = "destination"

        # Mock the name attribute...
        mock1 = mock.Mock()
        mock2 = mock.Mock()
        mock1.name = source
        mock2.name = destination

        mock_tempfile.return_value.__enter__.side_effect = [mock1, mock2]

        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout.readline = lambda: b""
        mock_proc.wait.return_value = None
        mock_popen = mock.MagicMock()
        mock_popen.return_value.__enter__.return_value = mock_proc

        mock_subprocess.Popen = mock_popen
        mock_subprocess.PIPE = "pipe"
        mock_subprocess.STDOUT = "stdout"

        op = GCSFileTransformOperator(
            task_id=TASK_ID,
            source_bucket=source_bucket,
            source_object=source_object,
            destination_object=destination_object,
            destination_bucket=destination_bucket,
            transform_script=transform_script,
        )
        op.execute(context=mock.MagicMock())

        mock_hook.return_value.download.assert_called_once_with(
            bucket_name=source_bucket, object_name=source_object, filename=source
        )

        mock_subprocess.Popen.assert_called_once_with(
            args=[transform_script, source, destination],
            stdout="pipe",
            stderr="stdout",
            close_fds=True,
        )

        mock_hook.return_value.upload.assert_called_with(
            bucket_name=destination_bucket,
            object_name=destination_object,
            filename=destination,
        )

    def test_get_openlineage_facets_on_start(self):
        expected_input = Dataset(
            namespace=f"gs://{TEST_BUCKET}",
            name="folder/a.txt",
        )
        expected_output = Dataset(
            namespace=f"gs://{TEST_BUCKET}2",
            name="b.txt",
        )

        operator = GCSFileTransformOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object="folder/a.txt",
            destination_bucket=f"{TEST_BUCKET}2",
            destination_object="b.txt",
            transform_script="/path/to_script",
        )

        lineage = operator.get_openlineage_facets_on_start()
        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0] == expected_input
        assert lineage.outputs[0] == expected_output


class TestGCSTimeSpanFileTransformOperatorDateInterpolation:
    def test_execute(self):
        interp_dt = datetime(2015, 2, 1, 15, 16, 17, 345, tzinfo=timezone.utc)

        assert GCSTimeSpanFileTransformOperator.interpolate_prefix(None, interp_dt) is None

        assert (
            GCSTimeSpanFileTransformOperator.interpolate_prefix("prefix_without_date", interp_dt)
            == "prefix_without_date"
        )

        assert (
            GCSTimeSpanFileTransformOperator.interpolate_prefix("prefix_with_year_%Y", interp_dt)
            == "prefix_with_year_2015"
        )

        assert (
            GCSTimeSpanFileTransformOperator.interpolate_prefix(
                "prefix_with_year_month_day/%Y/%m/%d/", interp_dt
            )
            == "prefix_with_year_month_day/2015/02/01/"
        )

        assert (
            GCSTimeSpanFileTransformOperator.interpolate_prefix(
                "prefix_with_year_month_day_and_percent_%%/%Y/%m/%d/", interp_dt
            )
            == "prefix_with_year_month_day_and_percent_%/2015/02/01/"
        )


class TestGCSTimeSpanFileTransformOperator:
    @mock.patch("airflow.providers.google.cloud.operators.gcs.TemporaryDirectory")
    @mock.patch("airflow.providers.google.cloud.operators.gcs.subprocess")
    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_execute(self, mock_hook, mock_subprocess, mock_tempdir):
        source_bucket = TEST_BUCKET
        source_prefix = "source_prefix"
        source_gcp_conn_id = ""

        destination_bucket = TEST_BUCKET + "_dest"
        destination_prefix = "destination_prefix"
        destination_gcp_conn_id = ""

        transform_script = "script.py"

        source = "source"
        destination = "destination"

        file1 = "file1"
        file2 = "file2"

        timespan_start = datetime(2015, 2, 1, 15, 16, 17, 345, tzinfo=timezone.utc)
        timespan_end = timespan_start + timedelta(hours=1)
        mock_dag = mock.Mock()
        mock_dag.next_dagrun_info.side_effect = [
            DagRunInfo(
                run_after=pendulum.instance(timespan_start),
                data_interval=DataInterval(
                    start=pendulum.instance(timespan_start),
                    end=pendulum.instance(timespan_end),
                ),
            ),
        ]
        mock_ti = mock.Mock()
        context = dict(
            execution_date=timespan_start,
            dag=mock_dag,
            ti=mock_ti,
        )

        mock_tempdir.return_value.__enter__.side_effect = [source, destination]
        mock_hook.return_value.list_by_timespan.return_value = [
            f"{source_prefix}/{file1}",
            f"{source_prefix}/{file2}",
        ]

        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout.readline = lambda: b""
        mock_proc.wait.return_value = None
        mock_popen = mock.MagicMock()
        mock_popen.return_value.__enter__.return_value = mock_proc

        mock_subprocess.Popen = mock_popen
        mock_subprocess.PIPE = "pipe"
        mock_subprocess.STDOUT = "stdout"

        op = GCSTimeSpanFileTransformOperator(
            task_id=TASK_ID,
            source_bucket=source_bucket,
            source_prefix=source_prefix,
            source_gcp_conn_id=source_gcp_conn_id,
            destination_bucket=destination_bucket,
            destination_prefix=destination_prefix,
            destination_gcp_conn_id=destination_gcp_conn_id,
            transform_script=transform_script,
        )

        with mock.patch.object(Path, "glob") as path_glob:
            path_glob.return_value.__iter__.return_value = [
                Path(f"{destination}/{file1}"),
                Path(f"{destination}/{file2}"),
            ]
            op.execute(context=context)

        mock_hook.return_value.list_by_timespan.assert_called_once_with(
            bucket_name=source_bucket,
            timespan_start=timespan_start,
            timespan_end=timespan_end,
            prefix=source_prefix,
        )

        mock_hook.return_value.download.assert_has_calls(
            [
                mock.call(
                    bucket_name=source_bucket,
                    object_name=f"{source_prefix}/{file1}",
                    filename=f"{source}/{source_prefix}/{file1}",
                    chunk_size=None,
                    num_max_attempts=1,
                ),
                mock.call(
                    bucket_name=source_bucket,
                    object_name=f"{source_prefix}/{file2}",
                    filename=f"{source}/{source_prefix}/{file2}",
                    chunk_size=None,
                    num_max_attempts=1,
                ),
            ]
        )

        mock_subprocess.Popen.assert_called_once_with(
            args=[
                transform_script,
                source,
                destination,
                timespan_start.replace(microsecond=0).isoformat(),
                timespan_end.replace(microsecond=0).isoformat(),
            ],
            stdout="pipe",
            stderr="stdout",
            close_fds=True,
        )

        mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call(
                    bucket_name=destination_bucket,
                    filename=f"{destination}/{file1}",
                    object_name=f"{destination_prefix}/{file1}",
                    chunk_size=None,
                    num_max_attempts=1,
                ),
                mock.call(
                    bucket_name=destination_bucket,
                    filename=f"{destination}/{file2}",
                    object_name=f"{destination_prefix}/{file2}",
                    chunk_size=None,
                    num_max_attempts=1,
                ),
            ]
        )

    @pytest.mark.parametrize(
        ("source_prefix", "dest_prefix", "inputs", "outputs"),
        (
            (
                None,
                None,
                [Dataset(f"gs://{TEST_BUCKET}", "/")],
                [Dataset(f"gs://{TEST_BUCKET}_dest", "/")],
            ),
            (
                None,
                "dest_pre/",
                [Dataset(f"gs://{TEST_BUCKET}", "/")],
                [Dataset(f"gs://{TEST_BUCKET}_dest", "dest_pre")],
            ),
            (
                "source_pre/",
                None,
                [Dataset(f"gs://{TEST_BUCKET}", "source_pre")],
                [Dataset(f"gs://{TEST_BUCKET}_dest", "/")],
            ),
            (
                "source_pre/",
                "dest_pre/",
                [Dataset(f"gs://{TEST_BUCKET}", "source_pre")],
                [Dataset(f"gs://{TEST_BUCKET}_dest", "dest_pre")],
            ),
            (
                "source_pre",
                "dest_pre",
                [Dataset(f"gs://{TEST_BUCKET}", "/")],
                [Dataset(f"gs://{TEST_BUCKET}_dest", "/")],
            ),
            (
                "dir1/source_pre",
                "dir2/dest_pre",
                [Dataset(f"gs://{TEST_BUCKET}", "dir1")],
                [Dataset(f"gs://{TEST_BUCKET}_dest", "dir2")],
            ),
            (
                "",
                "/",
                [Dataset(f"gs://{TEST_BUCKET}", "/")],
                [Dataset(f"gs://{TEST_BUCKET}_dest", "/")],
            ),
            (
                "source/a.txt",
                "target/",
                [Dataset(f"gs://{TEST_BUCKET}", "source/a.txt")],
                [Dataset(f"gs://{TEST_BUCKET}_dest", "target")],
            ),
        ),
        ids=(
            "no prefixes",
            "dest prefix only",
            "source prefix only",
            "both with ending slash",
            "both without ending slash",
            "both as directory with prefix",
            "both empty or root",
            "source prefix is file path",
        ),
    )
    @mock.patch("airflow.providers.google.cloud.operators.gcs.TemporaryDirectory")
    @mock.patch("airflow.providers.google.cloud.operators.gcs.subprocess")
    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_get_openlineage_facets_on_complete(
        self, mock_hook, mock_subprocess, mock_tempdir, source_prefix, dest_prefix, inputs, outputs
    ):
        source_bucket = TEST_BUCKET

        destination_bucket = TEST_BUCKET + "_dest"
        destination = "destination"

        file1 = "file1"
        file2 = "file2"

        timespan_start = datetime(2015, 2, 1, 15, 16, 17, 345, tzinfo=timezone.utc)
        mock_dag = mock.Mock()
        mock_dag.next_dagrun_info.side_effect = [
            DagRunInfo(
                run_after=pendulum.instance(timespan_start),
                data_interval=DataInterval(
                    start=pendulum.instance(timespan_start),
                    end=None,
                ),
            ),
        ]
        context = dict(
            execution_date=timespan_start,
            dag=mock_dag,
            ti=mock.Mock(),
        )

        mock_tempdir.return_value.__enter__.side_effect = ["source", destination]
        mock_hook.return_value.list_by_timespan.return_value = [
            f"{source_prefix or ''}{file1}",
            f"{source_prefix or ''}{file2}",
        ]

        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout.readline = lambda: b""
        mock_proc.wait.return_value = None
        mock_popen = mock.MagicMock()
        mock_popen.return_value.__enter__.return_value = mock_proc

        mock_subprocess.Popen = mock_popen
        mock_subprocess.PIPE = "pipe"
        mock_subprocess.STDOUT = "stdout"

        op = GCSTimeSpanFileTransformOperator(
            task_id=TASK_ID,
            source_bucket=source_bucket,
            source_prefix=source_prefix,
            source_gcp_conn_id="",
            destination_bucket=destination_bucket,
            destination_prefix=dest_prefix,
            destination_gcp_conn_id="",
            transform_script="script.py",
        )

        with mock.patch.object(Path, "glob") as path_glob:
            path_glob.return_value.__iter__.return_value = [
                Path(f"{destination}/{file1}"),
                Path(f"{destination}/{file2}"),
            ]
            op.execute(context=context)

        lineage = op.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == len(inputs)
        assert len(lineage.outputs) == len(outputs)
        assert all(element in lineage.inputs for element in inputs)
        assert all(element in inputs for element in lineage.inputs)
        assert all(element in lineage.outputs for element in outputs)
        assert all(element in outputs for element in lineage.outputs)


class TestGCSDeleteBucketOperator:
    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_delete_bucket(self, mock_hook):
        operator = GCSDeleteBucketOperator(task_id=TASK_ID, bucket_name=TEST_BUCKET)

        operator.execute(None)
        mock_hook.return_value.delete_bucket.assert_called_once_with(
            bucket_name=TEST_BUCKET, force=True, user_project=None
        )


class TestGoogleCloudStorageSync:
    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_execute(self, mock_hook):
        task = GCSSynchronizeBucketsOperator(
            task_id="task-id",
            source_bucket="SOURCE_BUCKET",
            destination_bucket="DESTINATION_BUCKET",
            source_object="SOURCE_OBJECT",
            destination_object="DESTINATION_OBJECT",
            recursive=True,
            delete_extra_files=True,
            allow_overwrite=True,
            gcp_conn_id="GCP_CONN_ID",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id="GCP_CONN_ID",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.sync.assert_called_once_with(
            source_bucket="SOURCE_BUCKET",
            source_object="SOURCE_OBJECT",
            destination_bucket="DESTINATION_BUCKET",
            destination_object="DESTINATION_OBJECT",
            delete_extra_files=True,
            recursive=True,
            allow_overwrite=True,
        )
