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

from airflow.providers.google.firebase.operators.firestore import CloudFirestoreExportDatabaseOperator

TEST_OUTPUT_URI_PREFIX: str = "gs://example-bucket/path"
TEST_PROJECT_ID: str = "test-project-id"

EXPORT_DOCUMENT_BODY = {
    "outputUriPrefix": "gs://test-bucket/test-naamespace/",
    "collectionIds": ["test-collection"],
}


class TestCloudFirestoreExportDatabaseOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.firebase.operators.firestore.CloudFirestoreHook")
    def test_execute(self, mock_firestore_hook):
        op = CloudFirestoreExportDatabaseOperator(
            task_id="test-task",
            body=EXPORT_DOCUMENT_BODY,
            gcp_conn_id="google_cloud_default",
            project_id=TEST_PROJECT_ID,
        )
        op.execute(mock.MagicMock())
        mock_firestore_hook.return_value.export_documents.assert_called_once_with(
            body=EXPORT_DOCUMENT_BODY, database_id="(default)", project_id=TEST_PROJECT_ID
        )
