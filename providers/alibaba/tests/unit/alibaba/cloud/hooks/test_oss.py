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

import alibabacloud_oss_v2 as oss

from airflow.providers.alibaba.cloud.hooks.oss import OSSHook

from unit.alibaba.cloud.utils.oss_mock import mock_oss_hook_default_project_id

OSS_STRING = "airflow.providers.alibaba.cloud.hooks.oss.{}"
MOCK_OSS_CONN_ID = "mock_id"
MOCK_BUCKET_NAME = "mock_bucket_name"
MOCK_KEY = "mock_key"
MOCK_KEYS = ["mock_key1", "mock_key2", "mock_key3"]
MOCK_CONTENT = "mock_content"
MOCK_FILE_PATH = "mock_file_path"


class TestOSSHook:
    def setup_method(self):
        with mock.patch(
            OSS_STRING.format("OSSHook.__init__"),
            new=mock_oss_hook_default_project_id,
        ):
            self.hook = OSSHook(oss_conn_id=MOCK_OSS_CONN_ID)

    def test_parse_oss_url(self):
        parsed = self.hook.parse_oss_url(f"oss://{MOCK_BUCKET_NAME}/this/is/not/a-real-key.txt")
        assert parsed == (MOCK_BUCKET_NAME, "this/is/not/a-real-key.txt"), "Incorrect parsing of the oss url"

    def test_parse_oss_object_directory(self):
        parsed = self.hook.parse_oss_url(f"oss://{MOCK_BUCKET_NAME}/this/is/not/a-real-oss-directory/")
        assert parsed == (
            MOCK_BUCKET_NAME,
            "this/is/not/a-real-oss-directory/",
        ), "Incorrect parsing of the oss url"

    @mock.patch(OSS_STRING.format("oss.credentials.StaticCredentialsProvider"))
    def test_get_credential(self, mock_provider):
        self.hook.get_credential()
        mock_provider.assert_called_once_with("mock_access_key_id", "mock_access_key_secret")

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_get_bucket(self, mock_get_client):
        self.hook.get_bucket("mock_bucket_name")
        mock_get_client.assert_called_once_with()

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_object_exist(self, mock_get_client):
        # Given
        mock_get_client.return_value.is_object_exist.return_value = True

        # When
        res = self.hook.object_exists(MOCK_KEY, MOCK_BUCKET_NAME)

        # Then
        assert res is True
        mock_get_client.assert_called_once_with()
        mock_get_client.return_value.is_object_exist.assert_called_once_with(
            bucket=MOCK_BUCKET_NAME, key=MOCK_KEY
        )

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_load_string(self, mock_get_client):
        self.hook.load_string(MOCK_KEY, MOCK_CONTENT, MOCK_BUCKET_NAME)
        mock_get_client.assert_called_once_with()
        request = mock_get_client.return_value.put_object.call_args.args[0]
        assert isinstance(request, oss.PutObjectRequest)
        assert request.bucket == MOCK_BUCKET_NAME
        assert request.key == MOCK_KEY
        assert request.body == MOCK_CONTENT.encode("utf-8")

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_upload_local_file(self, mock_get_client):
        self.hook.upload_local_file(MOCK_KEY, MOCK_FILE_PATH, MOCK_BUCKET_NAME)
        mock_get_client.assert_called_once_with()
        request, file_path = mock_get_client.return_value.uploader.return_value.upload_file.call_args.args
        assert isinstance(request, oss.PutObjectRequest)
        assert request.bucket == MOCK_BUCKET_NAME
        assert request.key == MOCK_KEY
        assert file_path == MOCK_FILE_PATH

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_download_file(self, mock_get_client):
        self.hook.download_file(MOCK_KEY, MOCK_FILE_PATH, MOCK_BUCKET_NAME)
        mock_get_client.assert_called_once_with()
        request, file_path = mock_get_client.return_value.downloader.return_value.download_file.call_args.args
        assert isinstance(request, oss.GetObjectRequest)
        assert request.bucket == MOCK_BUCKET_NAME
        assert request.key == MOCK_KEY
        assert file_path == MOCK_FILE_PATH

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_delete_object(self, mock_get_client):
        self.hook.delete_object(MOCK_KEY, MOCK_BUCKET_NAME)
        mock_get_client.assert_called_once_with()
        request = mock_get_client.return_value.delete_object.call_args.args[0]
        assert isinstance(request, oss.DeleteObjectRequest)
        assert request.bucket == MOCK_BUCKET_NAME
        assert request.key == MOCK_KEY

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_delete_objects(self, mock_get_client):
        self.hook.delete_objects(MOCK_KEYS, MOCK_BUCKET_NAME)
        mock_get_client.assert_called_once_with()
        request = mock_get_client.return_value.delete_multiple_objects.call_args.args[0]
        assert isinstance(request, oss.DeleteMultipleObjectsRequest)
        assert request.bucket == MOCK_BUCKET_NAME
        assert [obj.key for obj in request.objects] == MOCK_KEYS

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_delete_bucket(self, mock_get_client):
        self.hook.delete_bucket(MOCK_BUCKET_NAME)
        mock_get_client.assert_called_once_with()
        request = mock_get_client.return_value.delete_bucket.call_args.args[0]
        assert isinstance(request, oss.DeleteBucketRequest)
        assert request.bucket == MOCK_BUCKET_NAME

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_create_bucket(self, mock_get_client):
        self.hook.create_bucket(MOCK_BUCKET_NAME)
        mock_get_client.assert_called_once_with()
        request = mock_get_client.return_value.put_bucket.call_args.args[0]
        assert isinstance(request, oss.PutBucketRequest)
        assert request.bucket == MOCK_BUCKET_NAME

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_append_string(self, mock_get_client):
        self.hook.append_string(MOCK_BUCKET_NAME, MOCK_CONTENT, MOCK_KEY, 0)
        mock_get_client.assert_called_once_with()
        request = mock_get_client.return_value.append_object.call_args.args[0]
        assert isinstance(request, oss.AppendObjectRequest)
        assert request.bucket == MOCK_BUCKET_NAME
        assert request.key == MOCK_KEY
        assert request.position == 0
        assert request.body == MOCK_CONTENT.encode("utf-8")

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_read_key(self, mock_get_client):
        # Given
        mock_get_client.return_value.get_object.return_value.body.read.return_value = MOCK_CONTENT.encode(
            "utf-8"
        )

        # When
        res = self.hook.read_key(MOCK_BUCKET_NAME, MOCK_KEY)

        # Then
        assert res == MOCK_CONTENT
        mock_get_client.assert_called_once_with()
        request = mock_get_client.return_value.get_object.call_args.args[0]
        assert isinstance(request, oss.GetObjectRequest)
        assert request.bucket == MOCK_BUCKET_NAME
        assert request.key == MOCK_KEY
        mock_get_client.return_value.get_object.return_value.body.read.assert_called_once_with()

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_head_key(self, mock_get_client):
        self.hook.head_key(MOCK_BUCKET_NAME, MOCK_KEY)
        mock_get_client.assert_called_once_with()
        request = mock_get_client.return_value.head_object.call_args.args[0]
        assert isinstance(request, oss.HeadObjectRequest)
        assert request.bucket == MOCK_BUCKET_NAME
        assert request.key == MOCK_KEY

    @mock.patch(OSS_STRING.format("OSSHook._get_client"))
    def test_key_exists(self, mock_get_client):
        # When
        mock_get_client.return_value.is_object_exist.return_value = True

        # Given
        res = self.hook.key_exist(MOCK_BUCKET_NAME, MOCK_KEY)

        # Then
        assert res is True
        mock_get_client.assert_called_once_with()
        mock_get_client.return_value.is_object_exist.assert_called_once_with(
            bucket=MOCK_BUCKET_NAME, key=MOCK_KEY
        )

    def test_get_default_region(self):
        assert self.hook.get_default_region() == "mock_region"
