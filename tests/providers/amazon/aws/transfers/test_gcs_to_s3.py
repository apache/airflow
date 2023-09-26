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

from tempfile import NamedTemporaryFile
from unittest import mock

import pytest
from moto import mock_s3

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator

TASK_ID = "test-gcs-list-operator"
GCS_BUCKET = "test-bucket"
DELIMITER = ".csv"
PREFIX = "TEST"
S3_BUCKET = "s3://bucket/"
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
S3_ACL_POLICY = "private-read"
deprecated_call_match = "Usage of 'delimiter' is deprecated, please use 'match_glob' instead"


def _create_test_bucket():
    hook = S3Hook(aws_conn_id="airflow_gcs_test")
    # We're mocking all actual AWS calls and don't need a connection.
    # This avoids an Airflow warning about connection cannot be found.
    hook.get_connection = lambda _: None
    bucket = hook.get_bucket("bucket")
    bucket.create()
    return hook, bucket


@mock_s3
class TestGCSToS3Operator:
    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    def test_execute__match_glob(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            operator = GCSToS3Operator(
                task_id=TASK_ID,
                gcs_bucket=GCS_BUCKET,
                prefix=PREFIX,
                dest_aws_conn_id="aws_default",
                dest_s3_key=S3_BUCKET,
                replace=False,
                match_glob=f"**/*{DELIMITER}",
            )
            hook, bucket = _create_test_bucket()
            bucket.put_object(Key=MOCK_FILES[0], Body=b"testing")

            operator.execute(None)
            mock_hook.return_value.list.assert_called_once_with(
                bucket_name=GCS_BUCKET,
                delimiter=None,
                match_glob=f"**/*{DELIMITER}",
                prefix=PREFIX,
                user_project=None,
            )

    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    def test_execute_incremental(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            with pytest.deprecated_call(match=deprecated_call_match):
                operator = GCSToS3Operator(
                    task_id=TASK_ID,
                    gcs_bucket=GCS_BUCKET,
                    prefix=PREFIX,
                    delimiter=DELIMITER,
                    dest_aws_conn_id="aws_default",
                    dest_s3_key=S3_BUCKET,
                    replace=False,
                )
            hook, bucket = _create_test_bucket()
            bucket.put_object(Key=MOCK_FILES[0], Body=b"testing")

            # we expect all except first file in MOCK_FILES to be uploaded
            # and all the MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES[1:]) == sorted(uploaded_files)
            assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))

    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    def test_execute_without_replace(self, mock_hook):
        """
        Tests scenario where all the files are already in origin and destination without replace
        """
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            with pytest.deprecated_call(match=deprecated_call_match):
                operator = GCSToS3Operator(
                    task_id=TASK_ID,
                    gcs_bucket=GCS_BUCKET,
                    prefix=PREFIX,
                    delimiter=DELIMITER,
                    dest_aws_conn_id="aws_default",
                    dest_s3_key=S3_BUCKET,
                    replace=False,
                )
            hook, bucket = _create_test_bucket()
            for mock_file in MOCK_FILES:
                bucket.put_object(Key=mock_file, Body=b"testing")

            # we expect nothing to be uploaded
            # and all the MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert [] == uploaded_files
            assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))

    @pytest.mark.parametrize(
        argnames="dest_s3_url",
        argvalues=[f"{S3_BUCKET}/test/", f"{S3_BUCKET}/test"],
    )
    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    def test_execute_without_replace_with_folder_structure(self, mock_hook, dest_s3_url):
        mock_files_gcs = [f"test{idx}/{mock_file}" for idx, mock_file in enumerate(MOCK_FILES)]
        mock_files_s3 = [f"test/test{idx}/{mock_file}" for idx, mock_file in enumerate(MOCK_FILES)]
        mock_hook.return_value.list.return_value = mock_files_gcs

        hook, bucket = _create_test_bucket()
        for mock_file_s3 in mock_files_s3:
            bucket.put_object(Key=mock_file_s3, Body=b"testing")

        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            with pytest.deprecated_call(match=deprecated_call_match):
                operator = GCSToS3Operator(
                    task_id=TASK_ID,
                    gcs_bucket=GCS_BUCKET,
                    prefix=PREFIX,
                    delimiter=DELIMITER,
                    dest_aws_conn_id="aws_default",
                    dest_s3_key=dest_s3_url,
                    replace=False,
                )

            # we expect nothing to be uploaded
            # and all the MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)

            assert [] == uploaded_files
            assert sorted(mock_files_s3) == sorted(hook.list_keys("bucket", prefix="test/"))

    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    def test_execute(self, mock_hook):
        """
        Tests the scenario where there are no files in destination bucket
        """
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            with pytest.deprecated_call(match=deprecated_call_match):
                operator = GCSToS3Operator(
                    task_id=TASK_ID,
                    gcs_bucket=GCS_BUCKET,
                    prefix=PREFIX,
                    delimiter=DELIMITER,
                    dest_aws_conn_id="aws_default",
                    dest_s3_key=S3_BUCKET,
                    replace=False,
                )
            hook, _ = _create_test_bucket()

            # we expect all MOCK_FILES to be uploaded
            # and all MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES) == sorted(uploaded_files)
            assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))

    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    def test_execute_gcs_bucket_rename_compatibility(self, mock_hook):
        """
        Tests the same conditions as `test_execute` using the deprecated `bucket` parameter instead of
        `gcs_bucket`. This test can be removed when the `bucket` parameter is removed.
        """
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name
            bucket_param_deprecated_message = (
                "The ``bucket`` parameter is deprecated and will be removed in a future version. "
                "Please use ``gcs_bucket`` instead."
            )
            with pytest.deprecated_call(match=bucket_param_deprecated_message):
                operator = GCSToS3Operator(
                    task_id=TASK_ID,
                    bucket=GCS_BUCKET,
                    prefix=PREFIX,
                    match_glob=DELIMITER,
                    dest_aws_conn_id="aws_default",
                    dest_s3_key=S3_BUCKET,
                    replace=False,
                )
            hook, _ = _create_test_bucket()
            # we expect all MOCK_FILES to be uploaded
            # and all MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES) == sorted(uploaded_files)
            assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))
        with pytest.raises(ValueError) as excinfo:
            GCSToS3Operator(task_id=TASK_ID, dest_s3_key=S3_BUCKET)
        assert str(excinfo.value) == "You must pass either ``bucket`` or ``gcs_bucket``."

    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    def test_execute_with_replace(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            with pytest.deprecated_call(match=deprecated_call_match):
                operator = GCSToS3Operator(
                    task_id=TASK_ID,
                    gcs_bucket=GCS_BUCKET,
                    prefix=PREFIX,
                    delimiter=DELIMITER,
                    dest_aws_conn_id="aws_default",
                    dest_s3_key=S3_BUCKET,
                    replace=True,
                )
            hook, bucket = _create_test_bucket()
            for mock_file in MOCK_FILES:
                bucket.put_object(Key=mock_file, Body=b"testing")

            # we expect all MOCK_FILES to be uploaded and replace the existing ones
            # and all MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES) == sorted(uploaded_files)
            assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))

    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    def test_execute_incremental_with_replace(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            with pytest.deprecated_call(match=deprecated_call_match):
                operator = GCSToS3Operator(
                    task_id=TASK_ID,
                    gcs_bucket=GCS_BUCKET,
                    prefix=PREFIX,
                    delimiter=DELIMITER,
                    dest_aws_conn_id="aws_default",
                    dest_s3_key=S3_BUCKET,
                    replace=True,
                )
            hook, bucket = _create_test_bucket()
            for mock_file in MOCK_FILES[:2]:
                bucket.put_object(Key=mock_file, Body=b"testing")

            # we expect all the MOCK_FILES to be uploaded and replace the existing ones
            # and all MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES) == sorted(uploaded_files)
            assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))

    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.S3Hook")
    def test_execute_should_handle_with_default_dest_s3_extra_args(self, s3_mock_hook, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        mock_hook.return_value.download.return_value = b"testing"
        s3_mock_hook.return_value = mock.Mock()
        s3_mock_hook.parse_s3_url.return_value = mock.Mock()

        with pytest.deprecated_call(match=deprecated_call_match):
            operator = GCSToS3Operator(
                task_id=TASK_ID,
                gcs_bucket=GCS_BUCKET,
                prefix=PREFIX,
                delimiter=DELIMITER,
                dest_aws_conn_id="aws_default",
                dest_s3_key=S3_BUCKET,
                replace=True,
            )
        operator.execute(None)
        s3_mock_hook.assert_called_once_with(aws_conn_id="aws_default", extra_args={}, verify=None)

    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.S3Hook")
    def test_execute_should_pass_dest_s3_extra_args_to_s3_hook(self, s3_mock_hook, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name
            s3_mock_hook.return_value = mock.Mock()
            s3_mock_hook.parse_s3_url.return_value = mock.Mock()

            with pytest.deprecated_call(match=deprecated_call_match):
                operator = GCSToS3Operator(
                    task_id=TASK_ID,
                    gcs_bucket=GCS_BUCKET,
                    prefix=PREFIX,
                    delimiter=DELIMITER,
                    dest_aws_conn_id="aws_default",
                    dest_s3_key=S3_BUCKET,
                    replace=True,
                    dest_s3_extra_args={
                        "ContentLanguage": "value",
                    },
                )
            operator.execute(None)
            s3_mock_hook.assert_called_once_with(
                aws_conn_id="aws_default", extra_args={"ContentLanguage": "value"}, verify=None
            )

    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file")
    def test_execute_with_s3_acl_policy(self, mock_load_file, mock_gcs_hook):
        mock_gcs_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_gcs_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            with pytest.deprecated_call(match=deprecated_call_match):
                operator = GCSToS3Operator(
                    task_id=TASK_ID,
                    gcs_bucket=GCS_BUCKET,
                    prefix=PREFIX,
                    delimiter=DELIMITER,
                    dest_aws_conn_id="aws_default",
                    dest_s3_key=S3_BUCKET,
                    replace=False,
                    s3_acl_policy=S3_ACL_POLICY,
                )
            _create_test_bucket()
            operator.execute(None)

            # Make sure the acl_policy parameter is passed to the upload method
            _, kwargs = mock_load_file.call_args
            assert kwargs["acl_policy"] == S3_ACL_POLICY

    @mock.patch("airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook")
    def test_execute_without_keep_director_structure(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            with pytest.deprecated_call(match=deprecated_call_match):
                operator = GCSToS3Operator(
                    task_id=TASK_ID,
                    gcs_bucket=GCS_BUCKET,
                    prefix=PREFIX,
                    delimiter=DELIMITER,
                    dest_aws_conn_id="aws_default",
                    dest_s3_key=S3_BUCKET,
                    replace=False,
                    keep_directory_structure=False,
                )
            hook, _ = _create_test_bucket()

            # we expect all except first file in MOCK_FILES to be uploaded
            # and all the MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES) == sorted(uploaded_files)
            assert hook.check_for_prefix(bucket_name="bucket", prefix=PREFIX + "/", delimiter="/") is True
