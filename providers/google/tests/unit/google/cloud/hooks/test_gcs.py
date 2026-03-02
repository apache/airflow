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

import copy
import logging
import os
import re
from collections import namedtuple
from datetime import datetime, timedelta
from io import BytesIO
from unittest import mock
from unittest.mock import MagicMock

import dateutil
import google.cloud.storage as storage
import pytest
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.exceptions import NotFound
from google.cloud.storage.retry import DEFAULT_RETRY

from airflow.providers.common.compat.assets import Asset
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks import gcs
from airflow.providers.google.cloud.hooks.gcs import _fallback_object_url_to_object_name_and_bucket_name
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.version import version

from tests_common.test_utils.compat import timezone
from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
GCS_STRING = "airflow.providers.google.cloud.hooks.gcs.{}"

EMPTY_CONTENT = b""
PROJECT_ID_TEST = "project-id"
GCP_CONN_ID = "google_cloud_default"
GCS_FILE_URI = "gs://bucket/file"


@pytest.fixture(scope="module")
def testdata_bytes():
    # generate a 384KiB test file (larger than the minimum 256KiB multipart chunk size)
    return b"x" * 393216


@pytest.fixture(scope="module")
def testdata_string(testdata_bytes):
    return testdata_bytes.decode()


@pytest.fixture(scope="module")
def testdata_file(request, tmp_path_factory, testdata_bytes):
    fn = tmp_path_factory.mktemp(request.node.name) / "testfile_data"
    fn.write_bytes(testdata_bytes)
    return str(fn)


class TestGCSHookHelperFunctions:
    def test_parse_gcs_url(self):
        """
        Test GCS url parsing
        """

        assert gcs._parse_gcs_url("gs://bucket/path/to/blob") == ("bucket", "path/to/blob")

        # invalid URI
        with pytest.raises(AirflowException):
            gcs._parse_gcs_url("gs:/bucket/path/to/blob")
        with pytest.raises(AirflowException):
            gcs._parse_gcs_url("http://google.com/aaa")

        # trailing slash
        assert gcs._parse_gcs_url("gs://bucket/path/to/blob/") == ("bucket", "path/to/blob/")

        # bucket only
        assert gcs._parse_gcs_url("gs://bucket/") == ("bucket", "")

    @pytest.mark.parametrize(
        ("json_value", "parsed_value"),
        [
            ("[1, 2, 3]", [1, 2, 3]),
            ('"string value"', "string value"),
            ('{"key1": [1], "key2": {"subkey": 2}}', {"key1": [1], "key2": {"subkey": 2}}),
        ],
    )
    @mock.patch(GCS_STRING.format("GCSHook"))
    @mock.patch(GCS_STRING.format("NamedTemporaryFile"))
    def test_parse_json_from_gcs(self, temp_file, gcs_hook, json_value, parsed_value):
        temp_file.return_value.__enter__.return_value.read.return_value = json_value
        assert gcs.parse_json_from_gcs(gcp_conn_id=GCP_CONN_ID, file_uri=GCS_FILE_URI) == parsed_value

    @mock.patch(GCS_STRING.format("GCSHook"))
    def test_parse_json_from_gcs_fail_download(self, gsc_hook):
        gsc_hook.return_value.download.return_value.side_effect = GoogleAPICallError
        with pytest.raises(AirflowException):
            gcs.parse_json_from_gcs(gcp_conn_id=GCP_CONN_ID, file_uri=GCS_FILE_URI)

    @mock.patch(GCS_STRING.format("GCSHook"))
    @mock.patch(GCS_STRING.format("NamedTemporaryFile"))
    def test_parse_json_from_gcs_fail_read_file(self, temp_file, gcs_hook):
        for exception_class in (ValueError, OSError, RuntimeError):
            temp_file.return_value.__enter__.return_value.read.side_effect = exception_class
            with pytest.raises(AirflowException):
                gcs.parse_json_from_gcs(gcp_conn_id=GCP_CONN_ID, file_uri=GCS_FILE_URI)

    @mock.patch(GCS_STRING.format("GCSHook"))
    @mock.patch(GCS_STRING.format("NamedTemporaryFile"))
    def test_parse_json_from_gcs_fail_json_loads(self, temp_file, gcs_hook):
        temp_file.return_value.__enter__.return_value.read.return_value = "Invalid json"
        with pytest.raises(AirflowException):
            gcs.parse_json_from_gcs(gcp_conn_id=GCP_CONN_ID, file_uri=GCS_FILE_URI)


class TestFallbackObjectUrlToObjectNameAndBucketName:
    def setup_method(self):
        self.assertion_on_body = mock.MagicMock()

        @_fallback_object_url_to_object_name_and_bucket_name()
        def test_method(_, bucket_name=None, object_name=None, object_url=None):
            assert object_name == "OBJECT_NAME"
            assert bucket_name == "BUCKET_NAME"
            assert object_url is None
            self.assertion_on_body()

        self.test_method = test_method

    def test_should_url(self):
        self.test_method(None, object_url="gs://BUCKET_NAME/OBJECT_NAME")
        self.assertion_on_body.assert_called_once()

    def test_should_support_bucket_and_object(self):
        self.test_method(None, bucket_name="BUCKET_NAME", object_name="OBJECT_NAME")
        self.assertion_on_body.assert_called_once()

    def test_should_raise_exception_on_missing(self):
        with pytest.raises(
            TypeError,
            match=re.escape(
                "test_method() missing 2 required positional arguments: 'bucket_name' and 'object_name'"
            ),
        ):
            self.test_method(None)
        self.assertion_on_body.assert_not_called()

    def test_should_raise_exception_on_mutually_exclusive(self):
        with pytest.raises(AirflowException, match=re.escape("The mutually exclusive parameters.")):
            self.test_method(
                None,
                bucket_name="BUCKET_NAME",
                object_name="OBJECT_NAME",
                object_url="gs://BUCKET_NAME/OBJECT_NAME",
            )
        self.assertion_on_body.assert_not_called()


class TestGCSHook:
    def setup_method(self):
        with mock.patch(
            GCS_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.gcs_hook = gcs.GCSHook(gcp_conn_id="test")

    @mock.patch(
        BASE_STRING.format("GoogleBaseHook.get_credentials_and_project_id"),
        return_value=("CREDENTIALS", "PROJECT_ID"),
    )
    @mock.patch(GCS_STRING.format("GoogleBaseHook.get_connection"))
    @mock.patch("google.cloud.storage.Client")
    def test_storage_client_creation(self, mock_client, mock_get_connection, mock_get_creds_and_project_id):
        hook = gcs.GCSHook()
        result = hook.get_conn()
        # test that Storage Client is called with required arguments
        mock_client.assert_called_once_with(
            client_info=CLIENT_INFO, credentials="CREDENTIALS", project="PROJECT_ID"
        )
        assert mock_client.return_value == result

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_exists(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        # Given
        bucket_mock = mock_service.return_value.bucket
        blob_object = bucket_mock.return_value.blob
        exists_method = blob_object.return_value.exists
        exists_method.return_value = True

        # When
        response = self.gcs_hook.exists(bucket_name=test_bucket, object_name=test_object, user_project=None)

        # Then
        assert response
        bucket_mock.assert_called_once_with(test_bucket, user_project=None)
        blob_object.assert_called_once_with(blob_name=test_object)
        exists_method.assert_called_once_with(retry=DEFAULT_RETRY)

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_exists_nonexisting_object(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        # Given
        bucket_mock = mock_service.return_value.bucket
        blob_object = bucket_mock.return_value.blob
        exists_method = blob_object.return_value.exists
        exists_method.return_value = False

        # When
        response = self.gcs_hook.exists(bucket_name=test_bucket, object_name=test_object, user_project=None)

        # Then
        assert not response

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_is_updated_after(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        # Given
        mock_service.return_value.bucket.return_value.get_blob.return_value.updated = datetime(
            2019, 8, 28, 14, 7, 20, 700000, dateutil.tz.tzutc()
        )

        # When
        response = self.gcs_hook.is_updated_after(
            bucket_name=test_bucket, object_name=test_object, ts=datetime(2018, 1, 1, 1, 1, 1)
        )

        # Then
        assert response

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_is_updated_before(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        # Given
        mock_service.return_value.bucket.return_value.get_blob.return_value.updated = datetime(
            2019, 8, 28, 14, 7, 20, 700000, dateutil.tz.tzutc()
        )

        # When
        response = self.gcs_hook.is_updated_before(
            bucket_name=test_bucket, object_name=test_object, ts=datetime(2020, 1, 1, 1, 1, 1)
        )

        # Then
        assert response

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_is_updated_between(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        # Given
        mock_service.return_value.bucket.return_value.get_blob.return_value.updated = datetime(
            2019, 8, 28, 14, 7, 20, 700000, dateutil.tz.tzutc()
        )

        # When
        response = self.gcs_hook.is_updated_between(
            bucket_name=test_bucket,
            object_name=test_object,
            min_ts=datetime(2018, 1, 1, 1, 1, 1),
            max_ts=datetime(2020, 1, 1, 1, 1, 1),
        )

        # Then
        assert response

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_is_older_than_with_true_cond(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        # Given
        mock_service.return_value.bucket.return_value.get_blob.return_value.updated = datetime(
            2020, 1, 28, 14, 7, 20, 700000, dateutil.tz.tzutc()
        )

        # When
        response = self.gcs_hook.is_older_than(
            bucket_name=test_bucket,
            object_name=test_object,
            seconds=86400,  # 24hr
        )

        # Then
        assert response

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_is_older_than_with_false_cond(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        # Given

        mock_service.return_value.bucket.return_value.get_blob.return_value.updated = (
            timezone.utcnow() + timedelta(days=2)
        )

        # When
        response = self.gcs_hook.is_older_than(
            bucket_name=test_bucket,
            object_name=test_object,
            seconds=86400,  # 24hr
        )
        # Then
        assert not response

    @mock.patch("google.cloud.storage.Bucket")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_copy(self, mock_service, mock_bucket):
        source_bucket = "test-source-bucket"
        source_object = "test-source-object"
        destination_bucket = "test-dest-bucket"
        destination_object = "test-dest-object"

        destination_bucket_instance = mock_bucket
        source_blob = mock_bucket.blob(source_object)
        destination_blob = storage.Blob(bucket=destination_bucket_instance, name=destination_object)

        # Given
        bucket_mock = mock_service.return_value.bucket
        bucket_mock.return_value = mock_bucket
        copy_method = bucket_mock.return_value.copy_blob
        copy_method.return_value = destination_blob

        # When
        response = self.gcs_hook.copy(
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
        )

        # Then
        assert response is None
        copy_method.assert_called_once_with(
            blob=source_blob, destination_bucket=destination_bucket_instance, new_name=destination_object
        )

    def test_copy_fail_same_source_and_destination(self):
        source_bucket = "test-source-bucket"
        source_object = "test-source-object"
        destination_bucket = "test-source-bucket"
        destination_object = "test-source-object"

        with pytest.raises(
            ValueError,
            match="Either source/destination bucket or source/destination object must be different, "
            f"not both the same: bucket={source_bucket}, object={source_object}",
        ):
            self.gcs_hook.copy(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

    def test_copy_empty_source_bucket(self):
        source_bucket = None
        source_object = "test-source-object"
        destination_bucket = "test-dest-bucket"
        destination_object = "test-dest-object"

        with pytest.raises(ValueError, match="source_bucket and source_object cannot be empty."):
            self.gcs_hook.copy(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

    def test_copy_empty_source_object(self):
        source_bucket = "test-source-object"
        source_object = None
        destination_bucket = "test-dest-bucket"
        destination_object = "test-dest-object"

        with pytest.raises(ValueError, match="source_bucket and source_object cannot be empty."):
            self.gcs_hook.copy(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

    @mock.patch("google.cloud.storage.Bucket.copy_blob")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_copy_exposes_lineage(self, mock_service, mock_copy, hook_lineage_collector):
        source_bucket_name = "test-source-bucket"
        source_object_name = "test-source-object"
        destination_bucket_name = "test-dest-bucket"
        destination_object_name = "test-dest-object"

        source_bucket = storage.Bucket(mock_service, source_bucket_name)
        mock_copy.return_value = storage.Blob(
            name=destination_object_name, bucket=storage.Bucket(mock_service, destination_bucket_name)
        )
        mock_service.return_value.bucket.side_effect = lambda name: (
            source_bucket
            if name == source_bucket_name
            else storage.Bucket(mock_service, destination_bucket_name)
        )

        self.gcs_hook.copy(
            source_bucket=source_bucket_name,
            source_object=source_object_name,
            destination_bucket=destination_bucket_name,
            destination_object=destination_object_name,
        )

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri=f"gs://{source_bucket_name}/{source_object_name}"
        )
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"gs://{destination_bucket_name}/{destination_object_name}"
        )

    @mock.patch("google.cloud.storage.Bucket")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_rewrite(self, mock_service, mock_bucket):
        source_bucket = "test-source-bucket"
        source_object = "test-source-object"
        destination_bucket = "test-dest-bucket"
        destination_object = "test-dest-object"

        source_blob = mock_bucket.blob(source_object)

        # Given
        bucket_mock = mock_service.return_value.bucket
        bucket_mock.return_value = mock_bucket
        get_blob_method = bucket_mock.return_value.blob
        rewrite_method = get_blob_method.return_value.rewrite
        rewrite_method.side_effect = [(None, mock.ANY, mock.ANY), (mock.ANY, mock.ANY, mock.ANY)]

        # When
        response = self.gcs_hook.rewrite(
            source_bucket=source_bucket,
            source_object=source_object,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
        )

        # Then
        assert response is None
        rewrite_method.assert_called_once_with(source=source_blob)

    def test_rewrite_empty_source_bucket(self):
        source_bucket = None
        source_object = "test-source-object"
        destination_bucket = "test-dest-bucket"
        destination_object = "test-dest-object"

        with pytest.raises(ValueError, match="source_bucket and source_object cannot be empty."):
            self.gcs_hook.rewrite(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

    def test_rewrite_empty_source_object(self):
        source_bucket = "test-source-object"
        source_object = None
        destination_bucket = "test-dest-bucket"
        destination_object = "test-dest-object"

        with pytest.raises(ValueError, match="source_bucket and source_object cannot be empty."):
            self.gcs_hook.rewrite(
                source_bucket=source_bucket,
                source_object=source_object,
                destination_bucket=destination_bucket,
                destination_object=destination_object,
            )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_rewrite_exposes_lineage(self, mock_service, hook_lineage_collector):
        source_bucket_name = "test-source-bucket"
        source_object_name = "test-source-object"
        destination_bucket_name = "test-dest-bucket"
        destination_object_name = "test-dest-object"

        dest_bucket = storage.Bucket(mock_service, destination_bucket_name)
        blob = MagicMock(spec=storage.Blob)
        blob.rewrite = MagicMock(return_value=(None, None, None))
        dest_bucket.blob = MagicMock(return_value=blob)
        mock_service.return_value.bucket.side_effect = lambda name: (
            storage.Bucket(mock_service, source_bucket_name) if name == source_bucket_name else dest_bucket
        )

        self.gcs_hook.rewrite(
            source_bucket=source_bucket_name,
            source_object=source_object_name,
            destination_bucket=destination_bucket_name,
            destination_object=destination_object_name,
        )

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri=f"gs://{source_bucket_name}/{source_object_name}"
        )
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"gs://{destination_bucket_name}/{destination_object_name}"
        )

    @mock.patch("google.cloud.storage.Bucket")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_delete(self, mock_service, mock_bucket):
        test_bucket = "test_bucket"
        test_object = "test_object"
        blob_to_be_deleted = storage.Blob(name=test_object, bucket=mock_bucket)

        get_bucket_method = mock_service.return_value.get_bucket
        get_blob_method = get_bucket_method.return_value.get_blob
        delete_method = get_blob_method.return_value.delete
        delete_method.return_value = blob_to_be_deleted

        response = self.gcs_hook.delete(bucket_name=test_bucket, object_name=test_object)
        assert response is None

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_delete_nonexisting_object(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        bucket_method = mock_service.return_value.bucket
        blob = bucket_method.return_value.blob
        delete_method = blob.return_value.delete
        delete_method.side_effect = NotFound(message="Not Found")

        with pytest.raises(NotFound):
            self.gcs_hook.delete(bucket_name=test_bucket, object_name=test_object)

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_delete_exposes_lineage(self, mock_service, hook_lineage_collector):
        test_bucket = "test_bucket"
        test_object = "test_object"

        mock_service.return_value.bucket.return_value = storage.Bucket(mock_service, test_bucket)

        self.gcs_hook.delete(bucket_name=test_bucket, object_name=test_object)

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert len(hook_lineage_collector.collected_assets.outputs) == 0
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri=f"gs://{test_bucket}/{test_object}"
        )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_get_bucket(self, mock_service):
        test_bucket = "test bucket"

        self.gcs_hook.get_bucket(bucket_name=test_bucket)

        mock_service.return_value.bucket.assert_called_once_with(test_bucket)

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_delete_bucket(self, mock_service):
        test_bucket = "test bucket"

        self.gcs_hook.delete_bucket(bucket_name=test_bucket)

        mock_service.return_value.bucket.assert_called_once_with(test_bucket, user_project=None)
        mock_service.return_value.bucket.return_value.delete.assert_called_once()

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_delete_nonexisting_bucket(self, mock_service, caplog):
        mock_service.return_value.bucket.return_value.delete.side_effect = NotFound(message="Not Found")
        test_bucket = "test bucket"
        with caplog.at_level(logging.INFO):
            self.gcs_hook.delete_bucket(bucket_name=test_bucket)
        mock_service.return_value.bucket.assert_called_once_with(test_bucket, user_project=None)
        mock_service.return_value.bucket.return_value.delete.assert_called_once()
        assert "Bucket test bucket not exist" in caplog.text

    @mock.patch(GCS_STRING.format("GCSHook._get_blob"))
    def test_object_get_size(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"
        returned_file_size = 1200

        mock_blob = MagicMock()
        mock_blob.size = returned_file_size
        mock_service.return_value = mock_blob

        response = self.gcs_hook.get_size(bucket_name=test_bucket, object_name=test_object)

        assert response == returned_file_size

    @mock.patch(GCS_STRING.format("GCSHook._get_blob"))
    def test_object_get_crc32c(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"
        returned_file_crc32c = "xgdNfQ=="

        mock_blob = MagicMock()
        mock_blob.crc32c = returned_file_crc32c
        mock_service.return_value = mock_blob

        response = self.gcs_hook.get_crc32c(bucket_name=test_bucket, object_name=test_object)

        assert response == returned_file_crc32c

    @mock.patch(GCS_STRING.format("GCSHook._get_blob"))
    def test_object_get_md5hash(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"
        returned_file_md5hash = "leYUJBUWrRtks1UeUFONJQ=="

        mock_blob = MagicMock()
        mock_blob.md5_hash = returned_file_md5hash
        mock_service.return_value = mock_blob

        response = self.gcs_hook.get_md5hash(bucket_name=test_bucket, object_name=test_object)

        assert response == returned_file_md5hash

    @mock.patch(GCS_STRING.format("GCSHook._get_blob"))
    def test_object_get_metadata(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"
        returned_file_metadata = {"test_metadata_key": "test_metadata_val"}

        mock_blob = MagicMock()
        mock_blob.metadata = returned_file_metadata
        mock_service.return_value = mock_blob

        response = self.gcs_hook.get_metadata(bucket_name=test_bucket, object_name=test_object)

        assert response == returned_file_metadata

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_nonexisting_object_get_metadata(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        bucket_method = mock_service.return_value.bucket
        get_blob_method = bucket_method.return_value.get_blob
        get_blob_method.return_value = None

        with pytest.raises(ValueError, match=r"Object \((.*?)\) not found in Bucket \((.*?)\)"):
            self.gcs_hook.get_metadata(bucket_name=test_bucket, object_name=test_object)

    @mock.patch("google.cloud.storage.Bucket")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_create_bucket(self, mock_service, mock_bucket):
        test_bucket = "test_bucket"
        test_project = "test-project"
        test_location = "EU"
        test_labels = {"env": "prod"}
        test_storage_class = "MULTI_REGIONAL"

        labels_with_version = copy.deepcopy(test_labels)
        labels_with_version["airflow-version"] = "v" + version.replace(".", "-").replace("+", "-")

        mock_service.return_value.bucket.return_value.create.return_value = None
        mock_bucket.return_value.storage_class = test_storage_class
        mock_bucket.return_value.labels = labels_with_version

        sample_bucket = mock_service().bucket(bucket_name=test_bucket)

        response = self.gcs_hook.create_bucket(
            bucket_name=test_bucket,
            storage_class=test_storage_class,
            location=test_location,
            labels=test_labels,
            project_id=test_project,
        )

        assert response == sample_bucket.id

        assert sample_bucket.storage_class == test_storage_class
        assert sample_bucket.labels == test_labels

        mock_service.return_value.bucket.return_value.create.assert_called_once_with(
            project=test_project, location=test_location
        )

    @mock.patch("google.cloud.storage.Bucket")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_create_bucket_with_resource(self, mock_service, mock_bucket):
        test_bucket = "test_bucket"
        test_project = "test-project"
        test_location = "EU"
        test_labels = {"env": "prod"}
        test_storage_class = "MULTI_REGIONAL"
        test_versioning_enabled = {"enabled": True}

        mock_service.return_value.bucket.return_value.create.return_value = None
        mock_bucket.return_value.storage_class = test_storage_class
        mock_bucket.return_value.labels = test_labels
        mock_bucket.return_value.versioning_enabled = True

        sample_bucket = mock_service().bucket(bucket_name=test_bucket)

        # sample_bucket = storage.Bucket(client=mock_service, name=test_bucket)
        # Assert for resource other than None.
        response = self.gcs_hook.create_bucket(
            bucket_name=test_bucket,
            resource={"versioning": test_versioning_enabled},
            storage_class=test_storage_class,
            location=test_location,
            labels=test_labels,
            project_id=test_project,
        )
        assert response == sample_bucket.id

        mock_service.return_value.bucket.return_value._patch_property.assert_called_once_with(
            name="versioning", value=test_versioning_enabled
        )

        mock_service.return_value.bucket.return_value.create.assert_called_once_with(
            project=test_project, location=test_location
        )

    @mock.patch("google.cloud.storage.Bucket.blob")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_compose(self, mock_service, mock_blob):
        test_bucket = "test_bucket"
        test_source_objects = ["test_object_1", "test_object_2", "test_object_3"]
        test_destination_object = "test_object_composed"

        mock_service.return_value.bucket.return_value.blob.return_value = mock_blob(blob_name=mock.ANY)
        method = mock_service.return_value.bucket.return_value.blob.return_value.compose

        self.gcs_hook.compose(
            bucket_name=test_bucket,
            source_objects=test_source_objects,
            destination_object=test_destination_object,
        )

        method.assert_called_once_with(
            sources=[mock_blob(blob_name=source_object) for source_object in test_source_objects]
        )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_compose_with_empty_source_objects(self, mock_service):
        test_bucket = "test_bucket"
        test_source_objects = []
        test_destination_object = "test_object_composed"

        with pytest.raises(ValueError, match="source_objects cannot be empty."):
            self.gcs_hook.compose(
                bucket_name=test_bucket,
                source_objects=test_source_objects,
                destination_object=test_destination_object,
            )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_compose_without_bucket(self, mock_service):
        test_bucket = None
        test_source_objects = ["test_object_1", "test_object_2", "test_object_3"]
        test_destination_object = "test_object_composed"

        with pytest.raises(ValueError, match="bucket_name and destination_object cannot be empty."):
            self.gcs_hook.compose(
                bucket_name=test_bucket,
                source_objects=test_source_objects,
                destination_object=test_destination_object,
            )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_compose_without_destination_object(self, mock_service):
        test_bucket = "test_bucket"
        test_source_objects = ["test_object_1", "test_object_2", "test_object_3"]
        test_destination_object = None

        with pytest.raises(ValueError, match="bucket_name and destination_object cannot be empty."):
            self.gcs_hook.compose(
                bucket_name=test_bucket,
                source_objects=test_source_objects,
                destination_object=test_destination_object,
            )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_compose_exposes_lineage(self, mock_service, hook_lineage_collector):
        test_bucket = "test_bucket"
        source_object_names = ["test-source-object1", "test-source-object2"]
        destination_object_name = "test-dest-object"

        mock_service.return_value.bucket.return_value = storage.Bucket(mock_service, test_bucket)

        self.gcs_hook.compose(
            bucket_name=test_bucket,
            source_objects=source_object_names,
            destination_object=destination_object_name,
        )

        assert len(hook_lineage_collector.collected_assets.inputs) == 2
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri=f"gs://{test_bucket}/{source_object_names[0]}"
        )
        assert hook_lineage_collector.collected_assets.inputs[1].asset == Asset(
            uri=f"gs://{test_bucket}/{source_object_names[1]}"
        )
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"gs://{test_bucket}/{destination_object_name}"
        )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_download_as_bytes(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"
        test_object_bytes = BytesIO(b"input")

        download_method = mock_service.return_value.bucket.return_value.blob.return_value.download_as_bytes
        download_method.return_value = test_object_bytes

        response = self.gcs_hook.download(bucket_name=test_bucket, object_name=test_object, filename=None)

        assert response == test_object_bytes
        download_method.assert_called_once_with()

    @mock.patch("google.cloud.storage.Blob.download_as_bytes")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_download_as_bytes_exposes_lineage(self, mock_service, mock_download, hook_lineage_collector):
        source_bucket_name = "test-source-bucket"
        source_object_name = "test-source-object"

        mock_service.return_value.bucket.return_value = storage.Bucket(mock_service, source_bucket_name)

        self.gcs_hook.download(bucket_name=source_bucket_name, object_name=source_object_name, filename=None)

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert len(hook_lineage_collector.collected_assets.outputs) == 0
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri=f"gs://{source_bucket_name}/{source_object_name}"
        )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_download_to_file(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"
        test_object_bytes = BytesIO(b"input")
        test_file = "test_file"

        download_filename_method = (
            mock_service.return_value.bucket.return_value.blob.return_value.download_to_filename
        )
        download_filename_method.return_value = None

        download_as_a_bytes_method = (
            mock_service.return_value.bucket.return_value.blob.return_value.download_as_bytes
        )
        download_as_a_bytes_method.return_value = test_object_bytes
        response = self.gcs_hook.download(
            bucket_name=test_bucket, object_name=test_object, filename=test_file
        )

        assert response == test_file
        download_filename_method.assert_called_once_with(test_file, timeout=60)

    @mock.patch("google.cloud.storage.Blob.download_to_filename")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_download_to_file_exposes_lineage(self, mock_service, mock_download, hook_lineage_collector):
        source_bucket_name = "test-source-bucket"
        source_object_name = "test-source-object"
        file_name = "test.txt"

        mock_service.return_value.bucket.return_value = storage.Bucket(mock_service, source_bucket_name)

        self.gcs_hook.download(
            bucket_name=source_bucket_name, object_name=source_object_name, filename=file_name
        )

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri=f"gs://{source_bucket_name}/{source_object_name}"
        )
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(uri=f"file://{file_name}")

    @mock.patch(GCS_STRING.format("NamedTemporaryFile"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_provide_file(self, mock_service, mock_temp_file):
        test_bucket = "test_bucket"
        test_object = "test_object"
        test_object_bytes = BytesIO(b"input")
        test_file = "test_file"

        download_filename_method = (
            mock_service.return_value.bucket.return_value.blob.return_value.download_to_filename
        )
        download_filename_method.return_value = None

        download_as_a_bytes_method = (
            mock_service.return_value.bucket.return_value.blob.return_value.download_as_bytes
        )
        download_as_a_bytes_method.return_value = test_object_bytes
        mock_temp_file.return_value.__enter__.return_value = mock.MagicMock()
        mock_temp_file.return_value.__enter__.return_value.name = test_file

        with self.gcs_hook.provide_file(bucket_name=test_bucket, object_name=test_object) as response:
            assert test_file == response.name
        download_filename_method.assert_called_once_with(test_file, timeout=60)
        mock_temp_file.assert_has_calls(
            [
                mock.call(suffix="test_object", dir=None),
                mock.call().__enter__(),
                mock.call().__enter__().flush(),
                mock.call().__exit__(None, None, None),
            ]
        )

    @mock.patch(GCS_STRING.format("NamedTemporaryFile"))
    @mock.patch(GCS_STRING.format("GCSHook.upload"))
    def test_provide_file_upload(self, mock_upload, mock_temp_file):
        test_bucket = "test_bucket"
        test_object = "test_object"
        test_file = "test_file"

        mock_temp_file.return_value.__enter__.return_value = mock.MagicMock()
        mock_temp_file.return_value.__enter__.return_value.name = test_file

        with self.gcs_hook.provide_file_and_upload(
            bucket_name=test_bucket, object_name=test_object
        ) as fhandle:
            assert fhandle.name == test_file
            fhandle.write()

        mock_upload.assert_called_once_with(
            bucket_name=test_bucket, object_name=test_object, filename=test_file, user_project=None
        )
        mock_temp_file.assert_has_calls(
            [
                mock.call(suffix="test_object"),
                mock.call().__enter__(),
                mock.call().__enter__().write(),
                mock.call().__enter__().flush(),
                mock.call().__exit__(None, None, None),
            ]
        )

    @pytest.mark.parametrize(
        ("prefix", "blob_names", "returned_prefixes", "call_args", "result"),
        (
            (
                "prefix",
                ["prefix"],
                None,
                [mock.call(delimiter=",", prefix="prefix", versions=None, max_results=None, page_token=None)],
                ["prefix"],
            ),
            (
                "prefix",
                ["prefix"],
                {"prefix,"},
                [mock.call(delimiter=",", prefix="prefix", versions=None, max_results=None, page_token=None)],
                ["prefix,"],
            ),
            (
                ["prefix", "prefix_2"],
                ["prefix", "prefix2"],
                None,
                [
                    mock.call(
                        delimiter=",", prefix="prefix", versions=None, max_results=None, page_token=None
                    ),
                    mock.call(
                        delimiter=",", prefix="prefix_2", versions=None, max_results=None, page_token=None
                    ),
                ],
                ["prefix", "prefix2"],
            ),
        ),
    )
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_list__delimiter(self, mock_service, prefix, blob_names, returned_prefixes, call_args, result):
        Blob = namedtuple("Blob", ["name"])

        class BlobsIterator:
            def __init__(self):
                self._item_iter = (Blob(name=name) for name in blob_names)

            def __iter__(self):
                return self

            def __next__(self):
                try:
                    return next(self._item_iter)
                except StopIteration:
                    self.prefixes = returned_prefixes
                    self.next_page_token = None
                    raise

        mock_service.return_value.bucket.return_value.list_blobs.return_value = BlobsIterator()
        with pytest.deprecated_call():
            blobs = self.gcs_hook.list(
                bucket_name="test_bucket",
                prefix=prefix,
                delimiter=",",
            )
        assert mock_service.return_value.bucket.return_value.list_blobs.call_args_list == call_args
        assert blobs == result

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.functools")
    @mock.patch("google.cloud.storage.bucket._item_to_blob")
    @mock.patch("google.cloud.storage.bucket._blobs_page_start")
    @mock.patch("google.api_core.page_iterator.HTTPIterator")
    def test_list__match_glob(
        self, http_iterator, _blobs_page_start, _item_to_blob, mocked_functools, mock_service
    ):
        http_iterator.return_value.next_page_token = None
        self.gcs_hook.list(
            bucket_name="test_bucket",
            prefix="prefix",
            match_glob="**/*.json",
        )
        http_iterator.assert_has_calls(
            [
                mock.call(
                    api_request=mocked_functools.partial.return_value,
                    client=mock_service.return_value,
                    extra_params={"prefix": "prefix", "matchGlob": "**/*.json"},
                    item_to_value=_item_to_blob,
                    max_results=None,
                    page_start=_blobs_page_start,
                    page_token=None,
                    path=mock_service.return_value.bucket.return_value.path.__add__.return_value,
                )
            ]
        )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_list__error_match_glob_and_invalid_delimiter(self, _):
        with pytest.raises(AirflowException):
            self.gcs_hook.list(
                bucket_name="test_bucket",
                prefix="prefix",
                delimiter=",",
                match_glob="**/*.json",
            )

    @pytest.mark.parametrize("delimiter", [None, "", "/"])
    @mock.patch("google.api_core.page_iterator.HTTPIterator")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_list__error_match_glob_and_valid_delimiter(self, mock_service, http_iterator, delimiter):
        http_iterator.return_value.next_page_token = None
        self.gcs_hook.list(
            bucket_name="test_bucket",
            prefix="prefix",
            delimiter="/",
            match_glob="**/*.json",
        )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_list_by_timespans(self, mock_service):
        test_bucket = "test_bucket"

        # Given
        blob1 = mock.Mock()
        blob1.name = "in-interval-1"
        blob1.updated = datetime(2019, 8, 28, 14, 7, 20, 0, dateutil.tz.tzutc())
        blob2 = mock.Mock()
        blob2.name = "in-interval-2"
        blob2.updated = datetime(2019, 8, 28, 14, 37, 20, 0, dateutil.tz.tzutc())
        blob3 = mock.Mock()
        blob3.name = "outside-interval"
        blob3.updated = datetime(2019, 8, 29, 14, 7, 20, 0, dateutil.tz.tzutc())

        mock_service.return_value.bucket.return_value.list_blobs.return_value.__iter__.return_value = [
            blob1,
            blob2,
            blob3,
        ]
        mock_service.return_value.bucket.return_value.list_blobs.return_value.prefixes = None
        mock_service.return_value.bucket.return_value.list_blobs.return_value.next_page_token = None

        # When
        response = self.gcs_hook.list_by_timespan(
            bucket_name=test_bucket,
            timespan_start=datetime(2019, 8, 28, 14, 0, 0, 0, dateutil.tz.tzutc()),
            timespan_end=datetime(2019, 8, 28, 15, 0, 0, 0, dateutil.tz.tzutc()),
        )

        # Then
        assert len(response) == 2
        assert all("in-interval" in b for b in response)


class TestGCSHookUpload:
    def setup_method(self):
        with mock.patch(BASE_STRING.format("GoogleBaseHook.__init__")) as mock_init:
            mock_init.return_value = None
            self.gcs_hook = gcs.GCSHook(gcp_conn_id="test")

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_file(self, mock_service, testdata_file):
        test_bucket = "test_bucket"
        test_object = "test_object"
        metadata = {"key1": "val1", "key2": "key2"}

        bucket_mock = mock_service.return_value.bucket
        blob_object = bucket_mock.return_value.blob

        upload_method = blob_object.return_value.upload_from_filename

        self.gcs_hook.upload(test_bucket, test_object, filename=testdata_file, metadata=metadata)

        upload_method.assert_called_once_with(
            filename=testdata_file, content_type="application/octet-stream", timeout=60
        )

        assert metadata == blob_object.return_value.metadata

    @mock.patch("google.cloud.storage.Blob.upload_from_filename")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_file_exposes_lineage(self, mock_service, mock_upload, hook_lineage_collector):
        source_bucket_name = "test-source-bucket"
        source_object_name = "test-source-object"
        file_name = "test.txt"

        mock_service.return_value.bucket.return_value = storage.Bucket(mock_service, source_bucket_name)

        self.gcs_hook.upload(
            bucket_name=source_bucket_name, object_name=source_object_name, filename=file_name
        )

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"gs://{source_bucket_name}/{source_object_name}"
        )
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(uri=f"file://{file_name}")

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_cache_control(self, mock_service, testdata_file):
        test_bucket = "test_bucket"
        test_object = "test_object"
        cache_control = "public, max-age=3600"

        bucket_mock = mock_service.return_value.bucket
        blob_object = bucket_mock.return_value.blob

        self.gcs_hook.upload(test_bucket, test_object, filename=testdata_file, cache_control=cache_control)

        assert cache_control == blob_object.return_value.cache_control

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_file_gzip(self, mock_service, testdata_file):
        test_bucket = "test_bucket"
        test_object = "test_object"

        self.gcs_hook.upload(test_bucket, test_object, filename=testdata_file, gzip=True)
        assert not os.path.exists(testdata_file + ".gz")

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_data_str(self, mock_service, testdata_string):
        test_bucket = "test_bucket"
        test_object = "test_object"

        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_string

        self.gcs_hook.upload(test_bucket, test_object, data=testdata_string)

        upload_method.assert_called_once_with(testdata_string, content_type="text/plain", timeout=60)

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_empty_filename(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_filename

        self.gcs_hook.upload(test_bucket, test_object, filename="")

        upload_method.assert_called_once_with(
            filename="", content_type="application/octet-stream", timeout=60
        )

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_empty_data(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_string

        self.gcs_hook.upload(test_bucket, test_object, data="")

        upload_method.assert_called_once_with("", content_type="text/plain", timeout=60)

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_data_bytes(self, mock_service, testdata_bytes):
        test_bucket = "test_bucket"
        test_object = "test_object"

        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_string

        self.gcs_hook.upload(test_bucket, test_object, data=testdata_bytes)

        upload_method.assert_called_once_with(testdata_bytes, content_type="text/plain", timeout=60)

    @mock.patch("google.cloud.storage.Blob.upload_from_string")
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_data_exposes_lineage(self, mock_service, mock_upload, hook_lineage_collector):
        source_bucket_name = "test-source-bucket"
        source_object_name = "test-source-object"

        mock_service.return_value.bucket.return_value = storage.Bucket(mock_service, source_bucket_name)

        self.gcs_hook.upload(bucket_name=source_bucket_name, object_name=source_object_name, data="test")

        assert len(hook_lineage_collector.collected_assets.inputs) == 0
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"gs://{source_bucket_name}/{source_object_name}"
        )

    @mock.patch(GCS_STRING.format("BytesIO"))
    @mock.patch(GCS_STRING.format("gz.GzipFile"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_data_str_gzip(self, mock_service, mock_gzip, mock_bytes_io, testdata_string):
        test_bucket = "test_bucket"
        test_object = "test_object"
        encoding = "utf-8"

        gzip_ctx = mock_gzip.return_value.__enter__.return_value
        data = mock_bytes_io.return_value.getvalue.return_value
        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_string

        self.gcs_hook.upload(test_bucket, test_object, data=testdata_string, gzip=True)

        byte_str = bytes(testdata_string, encoding)
        mock_gzip.assert_called_once_with(fileobj=mock_bytes_io.return_value, mode="w")
        gzip_ctx.write.assert_called_once_with(byte_str)
        upload_method.assert_called_once_with(data, content_type="text/plain", timeout=60)

    @mock.patch(GCS_STRING.format("BytesIO"))
    @mock.patch(GCS_STRING.format("gz.GzipFile"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_data_bytes_gzip(self, mock_service, mock_gzip, mock_bytes_io, testdata_bytes):
        test_bucket = "test_bucket"
        test_object = "test_object"

        gzip_ctx = mock_gzip.return_value.__enter__.return_value
        data = mock_bytes_io.return_value.getvalue.return_value
        upload_method = mock_service.return_value.bucket.return_value.blob.return_value.upload_from_string

        self.gcs_hook.upload(test_bucket, test_object, data=testdata_bytes, gzip=True)

        mock_gzip.assert_called_once_with(fileobj=mock_bytes_io.return_value, mode="w")
        gzip_ctx.write.assert_called_once_with(testdata_bytes)
        upload_method.assert_called_once_with(data, content_type="text/plain", timeout=60)

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_upload_exceptions(self, mock_service, testdata_file, testdata_string):
        test_bucket = "test_bucket"
        test_object = "test_object"
        both_params_except = (
            "'filename' and 'data' parameter provided. Please "
            "specify a single parameter, either 'filename' for "
            "local file uploads or 'data' for file content uploads."
        )
        no_params_except = "'filename' and 'data' parameter missing. One is required to upload to gcs."

        with pytest.raises(ValueError, match=re.escape(no_params_except)):
            self.gcs_hook.upload(test_bucket, test_object)

        with pytest.raises(ValueError, match=re.escape(both_params_except)):
            self.gcs_hook.upload(test_bucket, test_object, filename=testdata_file, data=testdata_string)


class TestSyncGcsHook:
    def setup_method(self):
        with mock.patch(
            GCS_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.gcs_hook = gcs.GCSHook(gcp_conn_id="test")

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_do_nothing_when_buckets_is_empty(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = []
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET")
        mock_get_conn.return_value.bucket.assert_has_calls(
            [mock.call("SOURCE_BUCKET"), mock.call("DEST_BUCKET")]
        )
        source_bucket.list_blobs.assert_called_once_with(delimiter=None, prefix=None)
        destination_bucket.list_blobs.assert_called_once_with(delimiter=None, prefix=None)
        mock_delete.assert_not_called()
        mock_rewrite.assert_not_called()
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_append_slash_to_object_if_missing(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = []
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET",
            destination_bucket="DEST_BUCKET",
            source_object="SOURCE_OBJECT",
            destination_object="DESTINATION_OBJECT",
        )
        source_bucket.list_blobs.assert_called_once_with(delimiter=None, prefix="SOURCE_OBJECT/")
        destination_bucket.list_blobs.assert_called_once_with(delimiter=None, prefix="DESTINATION_OBJECT/")

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_copy_files(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1"),
            self._create_blob("FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET")
        mock_delete.assert_not_called()
        mock_rewrite.assert_has_calls(
            [
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_A",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_A",
                ),
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_B",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_B",
                ),
            ],
            any_order=True,
        )
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_copy_files_non_recursive(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1"),
            self._create_blob("AAA/FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", recursive=False)
        source_bucket.list_blobs.assert_called_once_with(delimiter="/", prefix=None)
        destination_bucket.list_blobs.assert_called_once_with(delimiter="/", prefix=None)

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_copy_files_to_subdirectory(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1"),
            self._create_blob("FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", destination_object="DEST_OBJ/"
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_has_calls(
            [
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_A",
                    destination_bucket="DEST_BUCKET",
                    destination_object="DEST_OBJ/FILE_A",
                ),
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_B",
                    destination_bucket="DEST_BUCKET",
                    destination_object="DEST_OBJ/FILE_B",
                ),
            ],
            any_order=True,
        )
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_copy_files_from_subdirectory(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1"),
            self._create_blob("SRC_OBJ/FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = []
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", source_object="SRC_OBJ/"
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_has_calls(
            [
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="SRC_OBJ/FILE_A",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_A",
                ),
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="SRC_OBJ/FILE_B",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_B",
                ),
            ],
            any_order=True,
        )
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_overwrite_files(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1"),
            self._create_blob("FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C2"),
            self._create_blob("FILE_B", "C2"),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", allow_overwrite=True
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_has_calls(
            [
                mock.call(
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_A",
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_A",
                ),
                mock.call(
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_B",
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_B",
                ),
            ],
            any_order=True,
        )
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_overwrite_cmek_files(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1", kms_key_name="KMS_KEY_1", generation=1),
            self._create_blob("FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C2", kms_key_name="KMS_KEY_2", generation=2),
            self._create_blob("FILE_B", "C2"),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", allow_overwrite=True
        )
        mock_delete.assert_not_called()
        source_bucket.get_blob.assert_called_once_with("FILE_A", generation=1)
        destination_bucket.get_blob.assert_called_once_with("FILE_A", generation=2)
        mock_rewrite.assert_has_calls(
            [
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_B",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_B",
                ),
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object=source_bucket.get_blob.return_value.name,
                    destination_bucket="DEST_BUCKET",
                    destination_object=source_bucket.get_blob.return_value.name.__getitem__.return_value,
                ),
            ],
            any_order=True,
        )
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_overwrite_files_to_subdirectory(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C1"),
            self._create_blob("FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("DEST_OBJ/FILE_A", "C2"),
            self._create_blob("DEST_OBJ/FILE_B", "C2"),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET",
            destination_bucket="DEST_BUCKET",
            destination_object="DEST_OBJ/",
            allow_overwrite=True,
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_has_calls(
            [
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_A",
                    destination_bucket="DEST_BUCKET",
                    destination_object="DEST_OBJ/FILE_A",
                ),
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="FILE_B",
                    destination_bucket="DEST_BUCKET",
                    destination_object="DEST_OBJ/FILE_B",
                ),
            ],
            any_order=True,
        )
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_overwrite_files_from_subdirectory(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1"),
            self._create_blob("SRC_OBJ/FILE_B", "C1"),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("FILE_A", "C2"),
            self._create_blob("FILE_B", "C2"),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET",
            destination_bucket="DEST_BUCKET",
            source_object="SRC_OBJ/",
            allow_overwrite=True,
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_has_calls(
            [
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="SRC_OBJ/FILE_A",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_A",
                ),
                mock.call(
                    source_bucket="SOURCE_BUCKET",
                    source_object="SRC_OBJ/FILE_B",
                    destination_bucket="DEST_BUCKET",
                    destination_object="FILE_B",
                ),
            ],
            any_order=True,
        )
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_delete_extra_files(self, mock_get_conn, mock_delete, mock_rewrite, mock_copy):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = []
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1", destination_bucket),
            self._create_blob("SRC_OBJ/FILE_B", "C1", destination_bucket),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", delete_extra_files=True
        )
        mock_delete.assert_has_calls(
            [mock.call("DEST_BUCKET", "SRC_OBJ/FILE_B"), mock.call("DEST_BUCKET", "SRC_OBJ/FILE_A")],
            any_order=True,
        )
        mock_rewrite.assert_not_called()
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_not_delete_extra_files_when_delete_extra_files_is_disabled(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = []
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1", destination_bucket),
            self._create_blob("SRC_OBJ/FILE_B", "C1", destination_bucket),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET", destination_bucket="DEST_BUCKET", delete_extra_files=False
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_not_called()
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.copy"))
    @mock.patch(GCS_STRING.format("GCSHook.rewrite"))
    @mock.patch(GCS_STRING.format("GCSHook.delete"))
    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_should_not_overwrite_when_overwrite_is_disabled(
        self, mock_get_conn, mock_delete, mock_rewrite, mock_copy
    ):
        source_bucket = self._create_bucket(name="SOURCE_BUCKET")
        source_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1", source_bucket),
            self._create_blob("SRC_OBJ/FILE_B", "C2", source_bucket),
        ]
        destination_bucket = self._create_bucket(name="DEST_BUCKET")
        destination_bucket.list_blobs.return_value = [
            self._create_blob("SRC_OBJ/FILE_A", "C1", destination_bucket),
            self._create_blob("SRC_OBJ/FILE_B", "C2", destination_bucket),
        ]
        mock_get_conn.return_value.bucket.side_effect = [source_bucket, destination_bucket]
        self.gcs_hook.sync(
            source_bucket="SOURCE_BUCKET",
            destination_bucket="DEST_BUCKET",
            delete_extra_files=False,
            allow_overwrite=False,
        )
        mock_delete.assert_not_called()
        mock_rewrite.assert_not_called()
        mock_copy.assert_not_called()

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_object_get_blob(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"
        mock_blob = mock.MagicMock()

        bucket_method = mock_service.return_value.bucket
        get_blob_method = bucket_method.return_value.get_blob
        get_blob_method.return_value = mock_blob

        response = self.gcs_hook._get_blob(bucket_name=test_bucket, object_name=test_object)

        assert response == mock_blob

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_nonexisting_object_get_blob(self, mock_service):
        test_bucket = "test_bucket"
        test_object = "test_object"

        bucket_method = mock_service.return_value.bucket
        get_blob_method = bucket_method.return_value.get_blob
        get_blob_method.return_value = None

        with pytest.raises(ValueError, match=r"Object \((.*?)\) not found in Bucket \((.*?)\)"):
            self.gcs_hook._get_blob(bucket_name=test_bucket, object_name=test_object)

    def _create_blob(
        self,
        name: str,
        crc32: str,
        bucket: MagicMock | None = None,
        kms_key_name: str | None = None,
        generation: int = 0,
        size: int = 9,
        updated: datetime | None = None,
    ):
        blob = mock.MagicMock(name=f"BLOB:{name}")
        blob.name = name
        blob.crc32 = crc32
        blob.bucket = bucket
        blob.kms_key_name = kms_key_name
        blob.generation = generation
        blob.size = size
        blob.updated = updated or timezone.utcnow()
        return blob

    def _create_bucket(self, name: str):
        bucket = mock.MagicMock(name=f"BUCKET:{name}")
        bucket.name = name
        return bucket

    @mock.patch(GCS_STRING.format("GCSHook.get_conn"))
    def test_sync_to_local_dir_behaviour(self, mock_get_conn, tmp_path):
        def get_logs_string(call_args_list):
            return "".join([args[0][0] % args[0][1:] for args in call_args_list])

        test_bucket = "test_bucket"
        mock_bucket = self._create_bucket(name=test_bucket)
        mock_get_conn.return_value.bucket.return_value = mock_bucket

        blobs = [
            self._create_blob("dag_01.py", "C1", mock_bucket),
            self._create_blob("dag_02.py", "C1", mock_bucket),
            self._create_blob("subproject1/dag_a.py", "C1", mock_bucket),
            self._create_blob("subproject1/dag_b.py", "C1", mock_bucket),
        ]
        mock_bucket.list_blobs.return_value = blobs

        sync_local_dir = tmp_path / "gcs_sync_dir"
        self.gcs_hook.log.debug = MagicMock()
        self.gcs_hook.download = MagicMock()

        self.gcs_hook.sync_to_local_dir(
            bucket_name=test_bucket, local_dir=sync_local_dir, prefix="", delete_stale=True
        )
        logs_string = get_logs_string(self.gcs_hook.log.debug.call_args_list)
        assert f"Downloading data from gs://{test_bucket}/ to {sync_local_dir}" in logs_string
        assert f"Local file {sync_local_dir}/dag_01.py does not exist." in logs_string
        assert f"Downloading dag_01.py to {sync_local_dir}/dag_01.py" in logs_string
        assert f"Local file {sync_local_dir}/subproject1/dag_a.py does not exist." in logs_string
        assert f"Downloading subproject1/dag_a.py to {sync_local_dir}/subproject1/dag_a.py" in logs_string
        assert self.gcs_hook.download.call_count == 4

        # Create dummy local files to simulate download
        for blob in blobs:
            p = sync_local_dir / blob.name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("test data")
            os.utime(p, (blob.updated.timestamp(), blob.updated.timestamp()))

        # add new file to bucket and sync
        self.gcs_hook.log.debug = MagicMock()
        self.gcs_hook.download.reset_mock()
        new_blob = self._create_blob("dag_03.py", "C1", mock_bucket)
        mock_bucket.list_blobs.return_value = blobs + [new_blob]
        self.gcs_hook.sync_to_local_dir(
            bucket_name=test_bucket, local_dir=sync_local_dir, prefix="", delete_stale=True
        )
        logs_string = get_logs_string(self.gcs_hook.log.debug.call_args_list)
        assert (
            f"Local file {sync_local_dir}/subproject1/dag_b.py is up-to-date with GCS object subproject1/dag_b.py. Skipping download."
            in logs_string
        )
        assert f"Local file {sync_local_dir}/dag_03.py does not exist." in logs_string
        assert f"Downloading dag_03.py to {sync_local_dir}/dag_03.py" in logs_string
        self.gcs_hook.download.assert_called_once()
        (sync_local_dir / "dag_03.py").write_text("test data")
        os.utime(
            sync_local_dir / "dag_03.py",
            (new_blob.updated.timestamp(), new_blob.updated.timestamp()),
        )

        # Test deletion of stale files
        local_file_that_should_be_deleted = sync_local_dir / "file_that_should_be_deleted.py"
        local_file_that_should_be_deleted.write_text("test dag")
        local_folder_should_be_deleted = sync_local_dir / "local_folder_should_be_deleted"
        local_folder_should_be_deleted.mkdir(exist_ok=True)
        self.gcs_hook.log.debug = MagicMock()
        self.gcs_hook.download.reset_mock()
        self.gcs_hook.sync_to_local_dir(
            bucket_name=test_bucket, local_dir=sync_local_dir, prefix="", delete_stale=True
        )
        logs_string = get_logs_string(self.gcs_hook.log.debug.call_args_list)
        assert f"Deleting stale local file: {local_file_that_should_be_deleted.as_posix()}" in logs_string
        assert f"Deleting stale empty directory: {local_folder_should_be_deleted.as_posix()}" in logs_string
        assert not self.gcs_hook.download.called

        # Test update of existing file (size change)
        self.gcs_hook.log.debug = MagicMock()
        self.gcs_hook.download.reset_mock()
        updated_blob = self._create_blob(
            "dag_03.py",
            "C2",
            mock_bucket,
            size=15,
        )
        mock_bucket.list_blobs.return_value = blobs + [updated_blob]
        self.gcs_hook.sync_to_local_dir(
            bucket_name=test_bucket, local_dir=sync_local_dir, prefix="", delete_stale=True
        )
        logs_string = get_logs_string(self.gcs_hook.log.debug.call_args_list)
        assert "GCS object size (15) and local file size (9) differ." in logs_string
        assert f"Downloading dag_03.py to {sync_local_dir}/dag_03.py" in logs_string
        self.gcs_hook.download.assert_called_once()
