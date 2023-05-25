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

import pytest

from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

TASK_ID = "test-s3-gcs-operator"
S3_BUCKET = "test-bucket"
S3_PREFIX = "TEST"
S3_DELIMITER = "/"
GCS_BUCKET = "gcs-bucket"
GCS_BUCKET_URI = "gs://" + GCS_BUCKET
GCS_PREFIX = "data/"
GCS_PATH_PREFIX = GCS_BUCKET_URI + "/" + GCS_PREFIX
MOCK_FILE_1 = "TEST1.csv"
MOCK_FILE_2 = "TEST2.csv"
MOCK_FILE_3 = "TEST3.csv"
MOCK_FILES = [MOCK_FILE_1, MOCK_FILE_2, MOCK_FILE_3]
AWS_CONN_ID = "aws_default"
GCS_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
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
        )

        assert operator.task_id == TASK_ID
        assert operator.bucket == S3_BUCKET
        assert operator.prefix == S3_PREFIX
        assert operator.delimiter == S3_DELIMITER
        assert operator.gcp_conn_id == GCS_CONN_ID
        assert operator.dest_gcs == GCS_PATH_PREFIX
        assert operator.google_impersonation_chain == IMPERSONATION_CHAIN
        assert operator.apply_gcs_prefix == APPLY_GCS_PREFIX

    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.S3Hook")
    @mock.patch("airflow.providers.amazon.aws.operators.s3.S3Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook")
    def test_execute(self, gcs_mock_hook, s3_one_mock_hook, s3_two_mock_hook):
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

        s3_one_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        s3_two_mock_hook.return_value.list_keys.return_value = MOCK_FILES

        uploaded_files = operator.execute(context={})
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call(GCS_BUCKET, GCS_PREFIX + MOCK_FILE_1, mock.ANY, gzip=False),
                mock.call(GCS_BUCKET, GCS_PREFIX + MOCK_FILE_2, mock.ANY, gzip=False),
                mock.call(GCS_BUCKET, GCS_PREFIX + MOCK_FILE_3, mock.ANY, gzip=False),
            ],
            any_order=True,
        )

        s3_one_mock_hook.assert_called_once_with(aws_conn_id=AWS_CONN_ID, verify=None)
        s3_two_mock_hook.assert_called_once_with(aws_conn_id=AWS_CONN_ID, verify=None)
        gcs_mock_hook.assert_called_once_with(
            gcp_conn_id=GCS_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        # we expect MOCK_FILES to be uploaded
        assert sorted(MOCK_FILES) == sorted(uploaded_files)

    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.S3Hook")
    @mock.patch("airflow.providers.amazon.aws.operators.s3.S3Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook")
    def test_execute_with_gzip(self, gcs_mock_hook, s3_one_mock_hook, s3_two_mock_hook):
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

        s3_one_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        s3_two_mock_hook.return_value.list_keys.return_value = MOCK_FILES

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
        "source_objects, existing_objects, objects_expected",
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
    @mock.patch("airflow.providers.amazon.aws.operators.s3.S3Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook")
    def test_execute_apply_gcs_prefix(
        self,
        gcs_mock_hook,
        s3_one_mock_hook,
        s3_two_mock_hook,
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

        s3_one_mock_hook.return_value.list_keys.return_value = [s3_prefix + s3_object]
        s3_two_mock_hook.return_value.list_keys.return_value = [s3_prefix + s3_object]

        uploaded_files = operator.execute(context={})
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call(GCS_BUCKET, gcs_object, mock.ANY, gzip=False),
            ],
            any_order=True,
        )

        s3_one_mock_hook.assert_called_once_with(aws_conn_id=AWS_CONN_ID, verify=None)
        s3_two_mock_hook.assert_called_once_with(aws_conn_id=AWS_CONN_ID, verify=None)
        gcs_mock_hook.assert_called_once_with(
            gcp_conn_id=GCS_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        assert sorted([s3_prefix + s3_object]) == sorted(uploaded_files)
