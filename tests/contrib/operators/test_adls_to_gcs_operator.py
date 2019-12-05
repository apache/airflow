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

from airflow.contrib.operators.adls_to_gcs import \
    AdlsToGoogleCloudStorageOperator
from tests.compat import mock

TASK_ID = 'test-adls-gcs-operator'
ADLS_PATH_1 = '*'
GCS_PATH = 'gs://test/'
MOCK_FILES = ["test/TEST1.csv", "test/TEST2.csv", "test/path/TEST3.csv",
              "test/path/PARQUET.parquet", "test/path/PIC.png"]
AZURE_CONN_ID = 'azure_data_lake_default'
GCS_CONN_ID = 'google_cloud_default'


class AdlsToGoogleCloudStorageOperatorTest(unittest.TestCase):
    def test_init(self):
        """Test AdlsToGoogleCloudStorageOperator instance is properly initialized."""

        operator = AdlsToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            src_adls=ADLS_PATH_1,
            dest_gcs=GCS_PATH,
            replace=False,
            azure_data_lake_conn_id=AZURE_CONN_ID,
            google_cloud_storage_conn_id=GCS_CONN_ID
        )

        self.assertEqual(operator.task_id, TASK_ID)
        self.assertEqual(operator.src_adls, ADLS_PATH_1)
        self.assertEqual(operator.dest_gcs, GCS_PATH)
        self.assertEqual(operator.replace, False)
        self.assertEqual(operator.google_cloud_storage_conn_id, GCS_CONN_ID)
        self.assertEqual(operator.azure_data_lake_conn_id, AZURE_CONN_ID)

    @mock.patch('airflow.contrib.operators.adls_to_gcs.AzureDataLakeHook')
    @mock.patch('airflow.contrib.operators.adls_list_operator.AzureDataLakeHook')
    @mock.patch(
        'airflow.contrib.operators.adls_to_gcs.GoogleCloudStorageHook')
    def test_execute(self, gcs_mock_hook, adls_one_mock_hook, adls_two_mock_hook):
        """Test the execute function when the run is successful."""

        operator = AdlsToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            src_adls=ADLS_PATH_1,
            dest_gcs=GCS_PATH,
            replace=False,
            azure_data_lake_conn_id=AZURE_CONN_ID,
            google_cloud_storage_conn_id=GCS_CONN_ID
        )

        adls_one_mock_hook.return_value.list.return_value = MOCK_FILES
        adls_two_mock_hook.return_value.list.return_value = MOCK_FILES

        uploaded_files = operator.execute(None)
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call(
                    bucket='test',
                    filename=mock.ANY,
                    object='test/path/PARQUET.parquet',
                    gzip=False
                ),
                mock.call(
                    bucket='test',
                    filename=mock.ANY,
                    object='test/path/TEST3.csv',
                    gzip=False
                ),
                mock.call(bucket='test', filename=mock.ANY, object='test/path/PIC.png', gzip=False),
                mock.call(bucket='test', filename=mock.ANY, object='test/TEST1.csv', gzip=False),
                mock.call(bucket='test', filename=mock.ANY, object='test/TEST2.csv', gzip=False)
            ], any_order=True
        )

        adls_one_mock_hook.assert_called_once_with(azure_data_lake_conn_id=AZURE_CONN_ID)
        adls_two_mock_hook.assert_called_once_with(azure_data_lake_conn_id=AZURE_CONN_ID)
        gcs_mock_hook.assert_called_once_with(
            google_cloud_storage_conn_id=GCS_CONN_ID, delegate_to=None)

        # we expect MOCK_FILES to be uploaded
        self.assertEqual(sorted(MOCK_FILES), sorted(uploaded_files))

    @mock.patch('airflow.contrib.operators.adls_to_gcs.AzureDataLakeHook')
    @mock.patch('airflow.contrib.operators.adls_list_operator.AzureDataLakeHook')
    @mock.patch(
        'airflow.contrib.operators.adls_to_gcs.GoogleCloudStorageHook')
    def test_execute_with_gzip(self, gcs_mock_hook, adls_one_mock_hook, adls_two_mock_hook):
        """Test the execute function when the run is successful."""

        operator = AdlsToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            src_adls=ADLS_PATH_1,
            dest_gcs=GCS_PATH,
            replace=False,
            azure_data_lake_conn_id=AZURE_CONN_ID,
            google_cloud_storage_conn_id=GCS_CONN_ID,
            gzip=True
        )

        adls_one_mock_hook.return_value.list.return_value = MOCK_FILES
        adls_two_mock_hook.return_value.list.return_value = MOCK_FILES

        # gcs_mock_hook.return_value.upload.side_effect = _assert_upload
        uploaded_files = operator.execute(None)
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call(
                    bucket='test',
                    filename=mock.ANY,
                    object='test/path/PARQUET.parquet',
                    gzip=True
                ),
                mock.call(
                    bucket='test',
                    filename=mock.ANY,
                    object='test/path/TEST3.csv',
                    gzip=True
                ),
                mock.call(bucket='test', filename=mock.ANY, object='test/path/PIC.png', gzip=True),
                mock.call(bucket='test', filename=mock.ANY, object='test/TEST1.csv', gzip=True),
                mock.call(bucket='test', filename=mock.ANY, object='test/TEST2.csv', gzip=True)
            ], any_order=True
        )

        # we expect MOCK_FILES to be uploaded
        self.assertEqual(sorted(MOCK_FILES), sorted(uploaded_files))


if __name__ == '__main__':
    unittest.main()
