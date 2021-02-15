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
from unittest import TestCase, mock

from airflow.providers.google.cloud.transfers.gdrive_to_local import GoogleDriveToLocalOperator

TASK_ID = "test-drive-to-local-operator"
FOLDER_ID = "1234567890qwerty"
FILE_NAME = "file.pdf"
OUTPUT_FILE = "out_file.pdf"


class TestGoogleDriveToLocalOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.transfers.gdrive_to_local.GoogleDriveHook")
    def test_execute(self, hook_mock):
        op = GoogleDriveToLocalOperator(
            task_id=TASK_ID,
            folder_id=FOLDER_ID,
            file_name=FILE_NAME,
            output_file=OUTPUT_FILE,
        )
        op.execute(context=None)
        hook_mock.assert_called_once()
