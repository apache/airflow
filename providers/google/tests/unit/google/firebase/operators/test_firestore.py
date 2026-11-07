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

from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.google.firebase.operators.firestore import CloudFirestoreExportDatabaseOperator
from airflow.providers.google.firebase.triggers.firestore import CloudFirestoreExportDatabaseTrigger

TEST_OUTPUT_URI_PREFIX: str = "gs://example-bucket/path"
TEST_PROJECT_ID: str = "test-project-id"

EXPORT_DOCUMENT_BODY = {
    "outputUriPrefix": "gs://test-bucket/test-namespace/",
    "collectionIds": ["test-collection"],
}


class TestCloudFirestoreExportDatabaseOperator:
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
            body=EXPORT_DOCUMENT_BODY,
            database_id="(default)",
            project_id=TEST_PROJECT_ID,
            reattach_on_restart=True,
        )

    @mock.patch("airflow.providers.google.firebase.operators.firestore.CloudFirestoreHook")
    def test_empty_body_fails_at_execute_time(self, mock_firestore_hook):
        op = CloudFirestoreExportDatabaseOperator(
            task_id="test-task",
            body="{{ var.value.export_body }}",
            gcp_conn_id="google_cloud_default",
            project_id=TEST_PROJECT_ID,
        )
        # Template rendering replaces the Jinja expression with the resolved value before execute.
        op.body = None

        with pytest.raises(ValueError, match="The required parameter 'body' is missing"):
            op.execute(mock.MagicMock())
        mock_firestore_hook.return_value.export_documents.assert_not_called()

    @mock.patch("airflow.providers.google.firebase.operators.firestore.CloudFirestoreHook")
    def test_execute_deferrable(self, mock_firestore_hook):
        mock_firestore_hook.return_value.start_export_documents.return_value = {
            "name": "projects/test-project-id/databases/(default)/operations/op-123",
            "done": False,
        }
        op = CloudFirestoreExportDatabaseOperator(
            task_id="test-task",
            body=EXPORT_DOCUMENT_BODY,
            gcp_conn_id="google_cloud_default",
            project_id=TEST_PROJECT_ID,
            deferrable=True,
            poll_interval=10,
        )

        with pytest.raises(TaskDeferred) as exc:
            op.execute(mock.MagicMock())

        assert isinstance(exc.value.trigger, CloudFirestoreExportDatabaseTrigger)
        assert exc.value.trigger.operation_name == (
            "projects/test-project-id/databases/(default)/operations/op-123"
        )
        assert exc.value.trigger.poll_interval == 10
        assert exc.value.method_name == "execute_complete"
        mock_firestore_hook.return_value.start_export_documents.assert_called_once_with(
            database_id="(default)",
            body=EXPORT_DOCUMENT_BODY,
            project_id=TEST_PROJECT_ID,
            reattach_on_restart=True,
        )

    @mock.patch("airflow.providers.google.firebase.operators.firestore.CloudFirestoreHook")
    def test_execute_deferrable_already_done(self, mock_firestore_hook):
        mock_firestore_hook.return_value.start_export_documents.return_value = {
            "name": "projects/test-project-id/databases/(default)/operations/op-123",
            "done": True,
            "response": {"outputUriPrefix": TEST_OUTPUT_URI_PREFIX},
        }
        op = CloudFirestoreExportDatabaseOperator(
            task_id="test-task",
            body=EXPORT_DOCUMENT_BODY,
            project_id=TEST_PROJECT_ID,
            deferrable=True,
        )

        result = op.execute(mock.MagicMock())

        assert result is None

    @mock.patch("airflow.providers.google.firebase.operators.firestore.CloudFirestoreHook")
    def test_execute_deferrable_already_done_with_error(self, mock_firestore_hook):
        mock_firestore_hook.return_value.start_export_documents.return_value = {
            "name": "projects/test-project-id/databases/(default)/operations/op-123",
            "done": True,
            "error": {"message": "Export failed"},
        }
        op = CloudFirestoreExportDatabaseOperator(
            task_id="test-task",
            body=EXPORT_DOCUMENT_BODY,
            project_id=TEST_PROJECT_ID,
            deferrable=True,
        )

        with pytest.raises(RuntimeError, match="Export failed"):
            op.execute(mock.MagicMock())

    def test_execute_complete_success(self):
        op = CloudFirestoreExportDatabaseOperator(
            task_id="test-task",
            body=EXPORT_DOCUMENT_BODY,
            project_id=TEST_PROJECT_ID,
            deferrable=True,
        )
        result = op.execute_complete(
            context=mock.MagicMock(),
            event={
                "status": "success",
                "operation_name": "op-123",
                "response": {"outputUriPrefix": TEST_OUTPUT_URI_PREFIX},
            },
        )
        assert result is None

    def test_execute_complete_error(self):
        op = CloudFirestoreExportDatabaseOperator(
            task_id="test-task",
            body=EXPORT_DOCUMENT_BODY,
            project_id=TEST_PROJECT_ID,
            deferrable=True,
        )
        with pytest.raises(RuntimeError, match="Operation failed"):
            op.execute_complete(
                context=mock.MagicMock(),
                event={
                    "status": "error",
                    "operation_name": "op-123",
                    "message": "Operation failed",
                },
            )
