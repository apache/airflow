# -*- coding: utf-8 -*-
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
import unittest
from unittest import mock

from airflow import AirflowException
from airflow.contrib.operators.gcs_to_gdrive_operator import GcsToGDriveOperator

MODULE = "airflow.contrib.operators.gcs_to_gdrive_operator"


class TestGcsToGDriveOperator(unittest.TestCase):
    @mock.patch(MODULE + ".GCSHook")
    @mock.patch(MODULE + ".GoogleDriveHook")
    @mock.patch(MODULE + ".tempfile.NamedTemporaryFile")
    def test_should_copy_single_file(self, mock_named_temporary_file, mock_gdrive, mock_gcs_hook):
        type(mock_named_temporary_file.return_value.__enter__.return_value).name = mock.PropertyMock(
            side_effect=["TMP1"]
        )
        task = GcsToGDriveOperator(
            task_id="copy_single_file",
            source_bucket="data",
            source_object="sales/sales-2017/january.avro",
            destination_object="copied_sales/2017/january-backup.avro",
        )

        task.execute(mock.MagicMock())

        mock_gcs_hook.assert_has_calls(
            [
                mock.call(delegate_to=None, google_cloud_storage_conn_id="google_cloud_default"),
                mock.call().download(
                    bucket_name="data", filename="TMP1", object_name="sales/sales-2017/january.avro"
                ),
            ]
        )

        mock_gdrive.assert_has_calls(
            [
                mock.call(delegate_to=None, gcp_conn_id="google_cloud_default"),
                mock.call().upload_file(
                    local_location="TMP1", remote_location="copied_sales/2017/january-backup.avro"
                ),
            ]
        )

    #
    @mock.patch(MODULE + ".GCSHook")
    @mock.patch(MODULE + ".GoogleDriveHook")
    @mock.patch(MODULE + ".tempfile.NamedTemporaryFile")
    def test_should_copy_files(self, mock_named_temporary_file, mock_gdrive, mock_gcs_hook):
        mock_gcs_hook.return_value.list.return_value = ["sales/A.avro", "sales/B.avro", "sales/C.avro"]
        type(mock_named_temporary_file.return_value.__enter__.return_value).name = mock.PropertyMock(
            side_effect=["TMP1", "TMP2", "TMP3"]
        )

        task = GcsToGDriveOperator(
            task_id="copy_files",
            source_bucket="data",
            source_object="sales/sales-2017/*.avro",
            destination_object="copied_sales/2017/",
        )

        task.execute(mock.MagicMock())
        mock_gcs_hook.assert_has_calls(
            [
                mock.call(delegate_to=None, google_cloud_storage_conn_id="google_cloud_default"),
                mock.call().list("data", delimiter=".avro", prefix="sales/sales-2017/"),
                mock.call().download(bucket_name="data", filename="TMP1", object_name="sales/A.avro"),
                mock.call().download(bucket_name="data", filename="TMP2", object_name="sales/B.avro"),
                mock.call().download(bucket_name="data", filename="TMP3", object_name="sales/C.avro"),
            ]
        )

        mock_gdrive.assert_has_calls(
            [
                mock.call(delegate_to=None, gcp_conn_id="google_cloud_default"),
                mock.call().upload_file(local_location="TMP1", remote_location="sales/A.avro"),
                mock.call().upload_file(local_location="TMP2", remote_location="sales/B.avro"),
                mock.call().upload_file(local_location="TMP3", remote_location="sales/C.avro"),
            ]
        )

    @mock.patch(MODULE + ".GCSHook")
    @mock.patch(MODULE + ".GoogleDriveHook")
    @mock.patch(MODULE + ".tempfile.NamedTemporaryFile")
    def test_should_move_files(self, mock_named_temporary_file, mock_gdrive, mock_gcs_hook):
        type(mock_named_temporary_file.return_value.__enter__.return_value).name = mock.PropertyMock(
            side_effect=["TMP1", "TMP2", "TMP3"]
        )
        mock_gcs_hook.return_value.list.return_value = ["sales/A.avro", "sales/B.avro", "sales/C.avro"]
        task = GcsToGDriveOperator(
            task_id="move_files",
            source_bucket="data",
            source_object="sales/sales-2017/*.avro",
            move_object=True,
        )

        task.execute(mock.MagicMock())
        mock_gcs_hook.assert_has_calls(
            [
                mock.call(delegate_to=None, google_cloud_storage_conn_id="google_cloud_default"),
                mock.call().list("data", delimiter=".avro", prefix="sales/sales-2017/"),
                mock.call().download(bucket_name="data", filename="TMP1", object_name="sales/A.avro"),
                mock.call().delete("data", "sales/A.avro"),
                mock.call().download(bucket_name="data", filename="TMP2", object_name="sales/B.avro"),
                mock.call().delete("data", "sales/B.avro"),
                mock.call().download(bucket_name="data", filename="TMP3", object_name="sales/C.avro"),
                mock.call().delete("data", "sales/C.avro"),
            ]
        )

        mock_gdrive.assert_has_calls(
            [
                mock.call(delegate_to=None, gcp_conn_id="google_cloud_default"),
                mock.call().upload_file(local_location="TMP1", remote_location="sales/A.avro"),
                mock.call().upload_file(local_location="TMP2", remote_location="sales/B.avro"),
                mock.call().upload_file(local_location="TMP3", remote_location="sales/C.avro"),
            ]
        )

    @mock.patch(MODULE + ".GCSHook")
    @mock.patch(MODULE + ".GoogleDriveHook")
    @mock.patch(MODULE + ".tempfile.NamedTemporaryFile")
    def test_should_raise_exception_on_multiple_wildcard(
        self, mock_named_temporary_file, mock_gdrive, mock_gcs_hook
    ):
        task = GcsToGDriveOperator(
            task_id="move_files", source_bucket="data", source_object="sales/*/*.avro", move_object=True
        )
        with self.assertRaisesRegex(AirflowException, "Only one wildcard"):
            task.execute(mock.MagicMock())
