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

from airflow.contrib.operators.gcs_download_operator \
    import GoogleCloudStorageDownloadOperator
from tests.compat import mock

TASK_ID = 'test-gcs-download-operator'
TEST_BUCKET = 'test-bucket'
TEST_OBJECT = 'dir1/test-object'
LOCAL_FILE_PATH = '/home/airflow/gcp/test-object'


class TestGoogleCloudStorageDownloadOperator(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageHook')
    def test_execute(self, mock_hook):
        operator = GoogleCloudStorageDownloadOperator(task_id=TASK_ID,
                                                      bucket=TEST_BUCKET,
                                                      object_name=TEST_OBJECT,
                                                      filename=LOCAL_FILE_PATH)

        operator.execute(None)
        mock_hook.return_value.download.assert_called_once_with(
            bucket_name=TEST_BUCKET, object_name=TEST_OBJECT, filename=LOCAL_FILE_PATH
        )
