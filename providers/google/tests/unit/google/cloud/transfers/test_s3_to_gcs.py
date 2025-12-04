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
from unittest.mock import PropertyMock

import pytest
import time_machine

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import CloudDataTransferServiceHook
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.utils.timezone import utcnow

PROJECT_ID = "test-project-id"
TASK_ID = "test-s3-gcs-operator"
S3_BUCKET = "test-bucket"
S3_PREFIX = "TEST"
S3_DELIMITER = "/"
GCS_BUCKET = "gcs-bucket"
GCS_BUCKET_URI = "gs://" + GCS_BUCKET
GCS_PREFIX = "data/"
GCS_BUCKET = "gcs-bucket"
GCS_BLOB = "data/"
GCS_PATH_PREFIX = f"gs://{GCS_BUCKET}/{GCS_BLOB}"
MOCK_FILE_1 = "TEST1.csv"
MOCK_FILE_2 = "TEST2.csv"
MOCK_FILE_3 = "TEST3.csv"
MOCK_FILES = [MOCK_FILE_1, MOCK_FILE_2, MOCK_FILE_3]
AWS_CONN_ID = "aws_default"
AWS_ACCESS_KEY_ID = "Mock AWS access key id"
AWS_SECRET_ACCESS_KEY = "Mock AWS secret access key"
GCS_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
DEFERRABLE = False
POLL_INTERVAL = 10
TRANSFER_JOB_ID_0 = "test-transfer-job-0"
TRANSFER_JOB_ID_1 = "test-transfer-job-1"
TRANSFER_JOBS = [TRANSFER_JOB_ID_0, TRANSFER_JOB_ID_1]
APPLY_GCS_PREFIX = False
PARAMETRIZED_OBJECT_PATHS = (
    "apply_gcs_prefix, s3_prefix, s3_object, gcs_destination, gcs_object",
    [
        (False, "", MOCK_FILE_1, GCS_PATH_PREFIX, GCS_PREFIX + MOCK_FILE_1),
        (False, S3_PREFIX, MOCK_FILE_1, GCS_PATH_PREFIX, GCS_PREFIX + S3_PREFIX + MOCK_FILE_1),
        (False, "", MOCK_FILE_1, GCS_BUCKET_URI, MOCK_FILE_1),
        (False, S3_PREFIX, MOCK_FILE_1, GCS_BUCKET_URI, S3_PREFIX + MOCK_FILE_1),
        (True, "", MOCK_FILE_1, GCS_PATH_PREFIX, GCS_PREFIX + MOCK_FILE_1),
        (True, S3_PREFIX, MOCK_FILE_1, GCS_PATH_PREFIX, GCS_PREFIX + MOCK_FILE_1),
        (True, "", MOCK_FILE_1, GCS_BUCKET_URI, MOCK_FILE_1),
        (True, S3_PREFIX, MOCK_FILE_1, GCS_BUCKET_URI, MOCK_FILE_1),
    ],
)


class TestS3ToGoogleCloudStorageOperator:
    def test_init(self):
        """Test S3ToGCSOperator instance is properly initialized."""

        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
            apply_gcs_prefix=APPLY_GCS_PREFIX,
            deferrable=DEFERRABLE,
            poll_interval=POLL_INTERVAL,
        )

        assert operator.task_id == TASK_ID
        assert operator.bucket == S3_BUCKET
        assert operator.prefix == S3_PREFIX
        assert operator.delimiter == S3_DELIMITER
        assert operator.gcp_conn_id == GCS_CONN_ID
        assert operator.dest_gcs == GCS_PATH_PREFIX
        assert operator.google_impersonation_chain == IMPERSONATION_CHAIN
        assert operator.apply_gcs_prefix == APPLY_GCS_PREFIX
        assert operator.deferrable == DEFERRABLE
        assert operator.poll_interval == POLL_INTERVAL

    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.S3Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook")
    def test_execute(self, gcs_mock_hook, s3_mock_hook):
        """Test the execute function when the run is successful."""

        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )
        operator.hook = mock.MagicMock()

        operator.hook.list_keys.return_value = MOCK_FILES

        uploaded_files = operator.execute(context={})
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call(GCS_BUCKET, GCS_PREFIX + MOCK_FILE_1, mock.ANY, gzip=False),
                mock.call(GCS_BUCKET, GCS_PREFIX + MOCK_FILE_2, mock.ANY, gzip=False),
                mock.call(GCS_BUCKET, GCS_PREFIX + MOCK_FILE_3, mock.ANY, gzip=False),
            ],
            any_order=True,
        )

        operator.hook.list_keys.assert_called_once()
        s3_mock_hook.assert_called_once_with(aws_conn_id=AWS_CONN_ID, verify=None)
        gcs_mock_hook.assert_called_once_with(
            gcp_conn_id=GCS_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        # we expect MOCK_FILES to be uploaded
        assert sorted(MOCK_FILES) == sorted(uploaded_files)

    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.S3Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook")
    def test_execute_with_gzip(self, gcs_mock_hook, s3_mock_hook):
        """Test the execute function when the run is successful."""

        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            gzip=True,
        )

        operator.hook = mock.MagicMock()

        operator.hook.list_keys.return_value = MOCK_FILES

        operator.execute(context={})
        gcs_mock_hook.assert_called_once_with(
            gcp_conn_id=GCS_CONN_ID,
            impersonation_chain=None,
        )
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call(GCS_BUCKET, GCS_PREFIX + MOCK_FILE_1, mock.ANY, gzip=True),
                mock.call(GCS_BUCKET, GCS_PREFIX + MOCK_FILE_2, mock.ANY, gzip=True),
                mock.call(GCS_BUCKET, GCS_PREFIX + MOCK_FILE_3, mock.ANY, gzip=True),
            ],
            any_order=True,
        )

    @pytest.mark.parametrize(
        ("source_objects", "existing_objects", "objects_expected"),
        [
            (MOCK_FILES, [], MOCK_FILES),
            (MOCK_FILES, [MOCK_FILE_1], [MOCK_FILE_2, MOCK_FILE_3]),
            (MOCK_FILES, [MOCK_FILE_1, MOCK_FILE_2], [MOCK_FILE_3]),
            (MOCK_FILES, [MOCK_FILE_3, MOCK_FILE_2], [MOCK_FILE_1]),
            (MOCK_FILES, MOCK_FILES, []),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook")
    def test_exclude_existing_objects(
        self, mock_gcs_hook, source_objects, existing_objects, objects_expected
    ):
        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            gzip=True,
        )
        mock_gcs_hook.list.return_value = existing_objects
        files_reduced = operator.exclude_existing_objects(s3_objects=source_objects, gcs_hook=mock_gcs_hook)
        assert set(files_reduced) == set(objects_expected)

    @pytest.mark.parametrize(*PARAMETRIZED_OBJECT_PATHS)
    def test_s3_to_gcs_object(self, apply_gcs_prefix, s3_prefix, s3_object, gcs_destination, gcs_object):
        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=s3_prefix,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=gcs_destination,
            gzip=True,
            apply_gcs_prefix=apply_gcs_prefix,
        )
        assert operator.s3_to_gcs_object(s3_object=s3_prefix + s3_object) == gcs_object

    @pytest.mark.parametrize(*PARAMETRIZED_OBJECT_PATHS)
    def test_gcs_to_s3_object(self, apply_gcs_prefix, s3_prefix, s3_object, gcs_destination, gcs_object):
        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=s3_prefix,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=gcs_destination,
            gzip=True,
            apply_gcs_prefix=apply_gcs_prefix,
        )
        assert operator.gcs_to_s3_object(gcs_object=gcs_object) == s3_prefix + s3_object

    @pytest.mark.parametrize(*PARAMETRIZED_OBJECT_PATHS)
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.S3Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook")
    def test_execute_apply_gcs_prefix(
        self,
        gcs_mock_hook,
        s3_mock_hook,
        apply_gcs_prefix,
        s3_prefix,
        s3_object,
        gcs_destination,
        gcs_object,
    ):
        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=s3_prefix,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=gcs_destination,
            google_impersonation_chain=IMPERSONATION_CHAIN,
            apply_gcs_prefix=apply_gcs_prefix,
        )
        operator.hook = mock.MagicMock()
        operator.hook.list_keys.return_value = [s3_prefix + s3_object]

        uploaded_files = operator.execute(context={})
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call(GCS_BUCKET, gcs_object, mock.ANY, gzip=False),
            ],
            any_order=True,
        )

        operator.hook.list_keys.assert_called_once()
        s3_mock_hook.assert_called_once_with(aws_conn_id=AWS_CONN_ID, verify=None)
        gcs_mock_hook.assert_called_once_with(
            gcp_conn_id=GCS_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        assert sorted([s3_prefix + s3_object]) == sorted(uploaded_files)

    @pytest.mark.parametrize(
        ("s3_prefix", "gcs_destination", "apply_gcs_prefix", "expected_input", "expected_output"),
        [
            ("dir/pre", "gs://bucket/dest_dir/", False, "dir/pre", "dest_dir/dir"),
            ("dir/pre/", "gs://bucket/dest_dir/", False, "dir/pre", "dest_dir/dir/pre"),
            ("dir/pre", "gs://bucket/dest_dir/", True, "dir/pre", "dest_dir"),
            ("dir/pre", "gs://bucket/", False, "dir/pre", "dir"),
            ("dir/pre", "gs://bucket/", True, "dir/pre", "/"),
            ("", "gs://bucket/", False, "/", "/"),
            ("", "gs://bucket/", True, "/", "/"),
        ],
    )
    def test_get_openlineage_facets_on_start(
        self, s3_prefix, gcs_destination, apply_gcs_prefix, expected_input, expected_output
    ):
        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=s3_prefix,
            dest_gcs=gcs_destination,
            apply_gcs_prefix=apply_gcs_prefix,
        )
        result = operator.get_openlineage_facets_on_start()
        assert not result.job_facets
        assert not result.run_facets
        assert len(result.outputs) == 1
        assert len(result.inputs) == 1
        assert result.outputs[0].namespace == "gs://bucket"
        assert result.outputs[0].name == expected_output
        assert result.inputs[0].namespace == f"s3://{S3_BUCKET}"
        assert result.inputs[0].name == expected_input


class TestS3ToGoogleCloudStorageOperatorDeferrable:
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.CloudDataTransferServiceHook")
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.S3Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook")
    def test_execute_deferrable(self, mock_gcs_hook, mock_s3_hook, mock_transfer_hook):
        mock_gcs_hook.return_value.project_id = PROJECT_ID

        mock_s3_super_hook = mock.MagicMock()
        mock_s3_super_hook.list_keys.return_value = MOCK_FILES
        mock_s3_hook.conn_config = mock.MagicMock(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        mock_create_transfer_job = mock.MagicMock()
        mock_create_transfer_job.return_value = dict(name=TRANSFER_JOB_ID_0)
        mock_transfer_hook.return_value.create_transfer_job = mock_create_transfer_job

        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            aws_conn_id=AWS_CONN_ID,
            replace=True,
            deferrable=True,
        )

        operator.hook = mock_s3_super_hook

        with pytest.raises(TaskDeferred) as exception_info:
            operator.execute(None)

        mock_s3_hook.assert_called_once_with(aws_conn_id=AWS_CONN_ID, verify=operator.verify)
        mock_s3_super_hook.list_keys.assert_called_once_with(
            bucket_name=S3_BUCKET, prefix=S3_PREFIX, delimiter=S3_DELIMITER, apply_wildcard=False
        )
        mock_create_transfer_job.assert_called_once()
        assert hasattr(exception_info.value, "trigger")
        trigger = exception_info.value.trigger
        assert trigger.project_id == PROJECT_ID
        assert trigger.job_names == [TRANSFER_JOB_ID_0]
        assert trigger.poll_interval == operator.poll_interval

        assert hasattr(exception_info.value, "method_name")
        assert exception_info.value.method_name == "execute_complete"

    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.S3Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook")
    def test_transfer_files_async(
        self,
        mock_s3_hook,
        mock_gcs_hook,
    ):
        mock_s3_hook.conn_config = mock.MagicMock(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        mock_gcs_hook.project_id = PROJECT_ID
        expected_job_names = [TRANSFER_JOB_ID_0]
        expected_method_name = "execute_complete"

        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
        )

        with mock.patch.object(operator, "submit_transfer_jobs") as mock_submit_transfer_jobs:
            mock_submit_transfer_jobs.return_value = expected_job_names
            with pytest.raises(TaskDeferred) as exception_info:
                operator.transfer_files_async(files=MOCK_FILES, gcs_hook=mock_gcs_hook, s3_hook=mock_s3_hook)

        mock_submit_transfer_jobs.assert_called_once_with(
            files=MOCK_FILES, gcs_hook=mock_gcs_hook, s3_hook=mock_s3_hook
        )

        assert hasattr(exception_info.value, "trigger")
        trigger = exception_info.value.trigger
        assert trigger.project_id == PROJECT_ID
        assert trigger.job_names == expected_job_names
        assert trigger.poll_interval == operator.poll_interval

        assert hasattr(exception_info.value, "method_name")
        assert exception_info.value.method_name == expected_method_name

    @pytest.mark.parametrize("invalid_poll_interval", [-5, 0])
    def test_init_error_polling_interval(self, invalid_poll_interval):
        operator = None
        expected_error_message = "Invalid value for poll_interval. Expected value greater than 0"
        with pytest.raises(ValueError, match=expected_error_message):
            operator = S3ToGCSOperator(
                task_id=TASK_ID,
                bucket=S3_BUCKET,
                prefix=S3_PREFIX,
                delimiter=S3_DELIMITER,
                gcp_conn_id=GCS_CONN_ID,
                dest_gcs=GCS_PATH_PREFIX,
                poll_interval=invalid_poll_interval,
            )
        assert operator is None

    def test_transfer_files_async_error_no_files(self):
        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
        )
        expected_error_message = "List of transferring files cannot be empty"
        with pytest.raises(ValueError, match=expected_error_message):
            operator.transfer_files_async(files=[], gcs_hook=mock.MagicMock(), s3_hook=mock.MagicMock())

    @pytest.mark.parametrize(
        ("file_names", "chunks", "expected_job_names"),
        [
            (MOCK_FILES, [MOCK_FILES], [TRANSFER_JOB_ID_0]),
            (
                [f"path/to/file{i}" for i in range(2000)],
                [
                    [f"path/to/file{i}" for i in range(1000)],
                    [f"path/to/file{i}" for i in range(1000, 2000)],
                ],
                TRANSFER_JOBS,
            ),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.S3Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook")
    def test_submit_transfer_jobs(
        self,
        mock_s3_hook,
        mock_gcs_hook,
        file_names,
        chunks,
        expected_job_names,
    ):
        mock_s3_hook.conn_config = mock.MagicMock(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        mock_gcs_hook.project_id = PROJECT_ID
        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
        )

        now_time = utcnow()
        with time_machine.travel(now_time):
            with mock.patch.object(operator, "get_transfer_hook") as mock_get_transfer_hook:
                mock_create_transfer_job = mock.MagicMock(
                    side_effect=[dict(name=job_name) for job_name in expected_job_names]
                )
                mock_get_transfer_hook.return_value = mock.MagicMock(
                    create_transfer_job=mock_create_transfer_job
                )
                job_names = operator.submit_transfer_jobs(
                    files=file_names,
                    gcs_hook=mock_gcs_hook,
                    s3_hook=mock_s3_hook,
                )

        mock_get_transfer_hook.assert_called_once()
        mock_create_transfer_job.assert_called()
        assert job_names == expected_job_names

    @mock.patch(
        "airflow.providers.google.cloud.transfers.s3_to_gcs.S3ToGCSOperator.log", new_callable=PropertyMock
    )
    def test_execute_complete_success(self, mock_log):
        expected_event_message = "Event message (success)"
        event = {
            "status": "success",
            "message": expected_event_message,
        }
        operator = S3ToGCSOperator(task_id=TASK_ID, bucket=S3_BUCKET)
        operator.execute_complete(context=mock.MagicMock(), event=event)

        mock_log.return_value.info.assert_called_once_with(
            "%s completed with response %s ", TASK_ID, event["message"]
        )

    @mock.patch(
        "airflow.providers.google.cloud.transfers.s3_to_gcs.S3ToGCSOperator.log", new_callable=PropertyMock
    )
    def test_execute_complete_error(self, mock_log):
        expected_event_message = "Event error message"
        event = {
            "status": "error",
            "message": expected_event_message,
        }
        operator = S3ToGCSOperator(task_id=TASK_ID, bucket=S3_BUCKET)
        with pytest.raises(AirflowException, match=expected_event_message):
            operator.execute_complete(context=mock.MagicMock(), event=event)

        mock_log.return_value.info.assert_not_called()

    @pytest.mark.db_test
    def test_get_transfer_hook(self):
        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )
        transfer_hook = operator.get_transfer_hook()

        assert isinstance(transfer_hook, CloudDataTransferServiceHook)
        assert transfer_hook.gcp_conn_id == GCS_CONN_ID
        assert transfer_hook.impersonation_chain == IMPERSONATION_CHAIN
