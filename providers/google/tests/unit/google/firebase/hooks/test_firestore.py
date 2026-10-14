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
"""
Tests for Google Cloud Firestore
"""

from __future__ import annotations

from unittest import mock
from unittest.mock import PropertyMock

import httplib2
import pytest
from googleapiclient.errors import HttpError

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.firebase.hooks.firestore import (
    CloudFirestoreAsyncHook,
    CloudFirestoreHook,
)

from unit.google.cloud.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

EXPORT_DOCUMENT_BODY = {
    "outputUriPrefix": "gs://test-bucket/test-namespace/",
    "collectionIds": ["test-collection"],
}

TEST_OPERATION = {
    "name": "operation-name",
}
TEST_WAITING_OPERATION = {"done": False, "response": "response"}
TEST_DONE_OPERATION = {"done": True, "response": "response"}
TEST_ERROR_OPERATION = {"done": True, "response": "response", "error": "error"}
TEST_PROJECT_ID = "firestore--project-id"


class TestCloudFirestoreHookWithPassedProjectId:
    hook: CloudFirestoreHook | None = None

    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudFirestoreHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_client_options")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook._authorize")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.build")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.build_from_document")
    def test_client_creation(
        self, mock_build_from_document, mock_build, mock_authorize, mock_get_client_options
    ):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "firestore", "v1", cache_discovery=False, client_options=mock_get_client_options.return_value
        )
        mock_build_from_document.assert_called_once_with(
            mock_build.return_value._rootDesc, http=mock_authorize.return_value
        )
        assert mock_build_from_document.return_value == result
        assert self.hook._conn == result

    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    def test_immediately_complete(self, get_conn_mock):
        service_mock = get_conn_mock.return_value

        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        mock_operation_get = (
            service_mock.projects.return_value.databases.return_value.operations.return_value.get
        )
        (mock_export_documents.return_value.execute.return_value) = TEST_OPERATION

        (mock_operation_get.return_value.execute.return_value) = TEST_DONE_OPERATION

        self.hook.export_documents(body=EXPORT_DOCUMENT_BODY, project_id=TEST_PROJECT_ID)

        mock_export_documents.assert_called_once_with(
            body=EXPORT_DOCUMENT_BODY, name="projects/firestore--project-id/databases/(default)"
        )

    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.time.sleep")
    def test_waiting_operation(self, _, get_conn_mock):
        service_mock = get_conn_mock.return_value

        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        mock_operation_get = (
            service_mock.projects.return_value.databases.return_value.operations.return_value.get
        )
        (mock_export_documents.return_value.execute.return_value) = TEST_OPERATION

        execute_mock = mock.Mock(
            **{"side_effect": [TEST_WAITING_OPERATION, TEST_DONE_OPERATION, TEST_DONE_OPERATION]}
        )
        mock_operation_get.return_value.execute = execute_mock

        self.hook.export_documents(body=EXPORT_DOCUMENT_BODY, project_id=TEST_PROJECT_ID)

        mock_export_documents.assert_called_once_with(
            body=EXPORT_DOCUMENT_BODY, name="projects/firestore--project-id/databases/(default)"
        )

    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.time.sleep")
    def test_error_operation(self, _, get_conn_mock):
        service_mock = get_conn_mock.return_value

        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        mock_operation_get = (
            service_mock.projects.return_value.databases.return_value.operations.return_value.get
        )
        (mock_export_documents.return_value.execute.return_value) = TEST_OPERATION

        execute_mock = mock.Mock(**{"side_effect": [TEST_WAITING_OPERATION, TEST_ERROR_OPERATION]})
        mock_operation_get.return_value.execute = execute_mock
        with pytest.raises(AirflowException, match="error"):
            self.hook.export_documents(body=EXPORT_DOCUMENT_BODY, project_id=TEST_PROJECT_ID)

    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    def test_list_operations(self, get_conn_mock):
        service_mock = get_conn_mock.return_value
        mock_list = service_mock.projects.return_value.databases.return_value.operations.return_value.list
        second_operation = {"name": "operation-name-2"}
        mock_list.return_value.execute.side_effect = [
            {"operations": [TEST_OPERATION], "nextPageToken": "page-2"},
            {"operations": [second_operation]},
        ]

        ops = self.hook.list_operations(project_id=TEST_PROJECT_ID)

        assert ops == [TEST_OPERATION, second_operation]
        assert mock_list.call_args_list == [
            mock.call(name="projects/firestore--project-id/databases/(default)"),
            mock.call(name="projects/firestore--project-id/databases/(default)", pageToken="page-2"),
        ]

    @pytest.mark.parametrize(
        ("body", "operations", "include_completed", "expected_name"),
        [
            pytest.param(
                EXPORT_DOCUMENT_BODY,
                [
                    "invalid-entry",
                    {
                        "name": "op-other-type",
                        "metadata": {"@type": "type.googleapis.com/ImportDocumentsMetadata"},
                    },
                    {
                        "name": "op-failed",
                        "metadata": {
                            "@type": "type.googleapis.com/ExportDocumentsMetadata",
                            "operationState": "FAILED",
                            "outputUriPrefix": "gs://test-bucket/test-namespace",
                            "collectionIds": ["test-collection"],
                        },
                    },
                    {
                        "name": "op-wrong-collection",
                        "metadata": {
                            "outputUriPrefix": "gs://test-bucket/test-namespace/",
                            "collectionIds": ["other"],
                        },
                    },
                    {
                        "name": "op-wrong-namespace",
                        "metadata": {
                            "outputUriPrefix": "gs://test-bucket/test-namespace/",
                            "collectionIds": ["test-collection"],
                            "namespaceIds": ["ns1"],
                        },
                    },
                    {
                        "name": "op-wrong-uri",
                        "metadata": {
                            "outputUriPrefix": "gs://other-bucket/path",
                            "collectionIds": ["test-collection"],
                        },
                    },
                    {
                        "name": "op-completed",
                        "done": True,
                        "metadata": "non-dict-metadata",
                        "response": {"outputUriPrefix": "gs://test-bucket/test-namespace"},
                    },
                    {
                        "name": "op-in-progress",
                        "done": False,
                        "metadata": {
                            "@type": "type.googleapis.com/google.firestore.admin.v1.ExportDocumentsMetadata",
                            "operationState": "PROCESSING",
                            "outputUriPrefix": "gs://test-bucket/test-namespace",
                            "collectionIds": ["test-collection"],
                        },
                    },
                ],
                True,
                "op-in-progress",
                id="prefers_in_progress_match",
            ),
            pytest.param(
                EXPORT_DOCUMENT_BODY,
                [
                    {
                        "name": "op-completed",
                        "done": True,
                        "metadata": {
                            "collectionIds": ["test-collection"],
                        },
                        "response": {"outputUriPrefix": "gs://test-bucket/test-namespace"},
                    }
                ],
                True,
                "op-completed",
                id="matches_completed_operation_via_response_uri",
            ),
            pytest.param(
                EXPORT_DOCUMENT_BODY,
                [
                    {
                        "name": "op-completed",
                        "done": True,
                        "metadata": {
                            "collectionIds": ["test-collection"],
                        },
                        "response": {"outputUriPrefix": "gs://test-bucket/test-namespace"},
                    }
                ],
                False,
                None,
                id="ignores_completed_when_include_completed_false",
            ),
            pytest.param(
                {"collectionIds": ["test-collection"]},
                [
                    {
                        "name": "op-done-no-target-uri",
                        "done": True,
                        "metadata": {"collectionIds": ["test-collection"]},
                    }
                ],
                True,
                None,
                id="returns_none_when_no_target_uri_specified",
            ),
        ],
    )
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.list_operations")
    def test_find_matching_export_operation(
        self, mock_list_ops, body, operations, include_completed, expected_name
    ):
        mock_list_ops.return_value = operations
        result = self.hook.find_matching_export_operation(
            body=body, project_id=TEST_PROJECT_ID, include_completed=include_completed
        )
        if expected_name is None:
            assert result is None
        else:
            assert result is not None
            assert result["name"] == expected_name

    @mock.patch(
        "airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.find_matching_export_operation"
    )
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    def test_export_documents_reattaches_to_in_progress_operation(self, get_conn_mock, mock_find_op):
        service_mock = get_conn_mock.return_value
        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        mock_operation_get = (
            service_mock.projects.return_value.databases.return_value.operations.return_value.get
        )
        mock_find_op.return_value = {"name": "existing-op", "done": False}
        mock_operation_get.return_value.execute.return_value = TEST_DONE_OPERATION

        self.hook.export_documents(body=EXPORT_DOCUMENT_BODY, project_id=TEST_PROJECT_ID)

        mock_export_documents.assert_not_called()
        mock_operation_get.assert_called_once_with(name="existing-op")

    @mock.patch(
        "airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.find_matching_export_operation"
    )
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    def test_export_documents_skips_when_already_completed_on_http_400(self, get_conn_mock, mock_find_op):
        service_mock = get_conn_mock.return_value
        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        mock_operation_get = (
            service_mock.projects.return_value.databases.return_value.operations.return_value.get
        )
        http_400 = HttpError(httplib2.Response({"status": 400}), b"Path already exists")
        mock_export_documents.return_value.execute.side_effect = http_400
        mock_find_op.side_effect = [
            None,
            {
                "name": "completed-op",
                "done": True,
                "response": {"outputUriPrefix": "x"},
            },
        ]

        self.hook.export_documents(body=EXPORT_DOCUMENT_BODY, project_id=TEST_PROJECT_ID)

        mock_export_documents.assert_called_once()
        mock_operation_get.assert_not_called()

    @mock.patch(
        "airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.find_matching_export_operation"
    )
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    def test_start_export_documents_recovers_on_http_400_race_in_progress(self, get_conn_mock, mock_find_op):
        service_mock = get_conn_mock.return_value
        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        http_400 = HttpError(httplib2.Response({"status": 400}), b"Operation already in progress")
        mock_export_documents.return_value.execute.side_effect = http_400
        mock_find_op.side_effect = [None, {"name": "race-op", "done": False}]

        op = self.hook.start_export_documents(body=EXPORT_DOCUMENT_BODY, project_id=TEST_PROJECT_ID)

        assert op == {"name": "race-op", "done": False}

    @mock.patch(
        "airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.find_matching_export_operation"
    )
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    def test_start_export_documents_reraises_http_error_when_no_match(self, get_conn_mock, mock_find_op):
        service_mock = get_conn_mock.return_value
        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        http_400 = HttpError(httplib2.Response({"status": 400}), b"Invalid argument")
        mock_export_documents.return_value.execute.side_effect = http_400
        mock_find_op.return_value = None

        with pytest.raises(HttpError):
            self.hook.start_export_documents(
                body=EXPORT_DOCUMENT_BODY, project_id=TEST_PROJECT_ID, reattach_on_restart=False
            )

    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.start_export_documents")
    def test_export_documents_raises_on_done_operation_with_error(self, mock_start_export):
        mock_start_export.return_value = {"name": "op-err", "done": True, "error": "Export failed"}
        with pytest.raises(RuntimeError, match="Export failed"):
            self.hook.export_documents(body=EXPORT_DOCUMENT_BODY, project_id=TEST_PROJECT_ID)


class TestCloudFirestoreHookWithDefaultProjectIdFromConnection:
    hook: CloudFirestoreHook | None = None

    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudFirestoreHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_client_options")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook._authorize")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.build")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.build_from_document")
    def test_client_creation(
        self, mock_build_from_document, mock_build, mock_authorize, mock_get_client_options
    ):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "firestore", "v1", cache_discovery=False, client_options=mock_get_client_options.return_value
        )
        mock_build_from_document.assert_called_once_with(
            mock_build.return_value._rootDesc, http=mock_authorize.return_value
        )
        assert mock_build_from_document.return_value == result
        assert self.hook._conn == result

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    def test_immediately_complete(self, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value

        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        mock_operation_get = (
            service_mock.projects.return_value.databases.return_value.operations.return_value.get
        )
        (mock_export_documents.return_value.execute.return_value) = TEST_OPERATION

        mock_operation_get.return_value.execute.return_value = TEST_DONE_OPERATION

        self.hook.export_documents(body=EXPORT_DOCUMENT_BODY)

        mock_export_documents.assert_called_once_with(
            body=EXPORT_DOCUMENT_BODY, name="projects/example-project/databases/(default)"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.time.sleep")
    def test_waiting_operation(self, _, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value

        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        mock_operation_get = (
            service_mock.projects.return_value.databases.return_value.operations.return_value.get
        )
        (mock_export_documents.return_value.execute.return_value) = TEST_OPERATION

        execute_mock = mock.Mock(
            **{"side_effect": [TEST_WAITING_OPERATION, TEST_DONE_OPERATION, TEST_DONE_OPERATION]}
        )
        mock_operation_get.return_value.execute = execute_mock

        self.hook.export_documents(body=EXPORT_DOCUMENT_BODY)

        mock_export_documents.assert_called_once_with(
            body=EXPORT_DOCUMENT_BODY, name="projects/example-project/databases/(default)"
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.time.sleep")
    def test_error_operation(self, _, get_conn_mock, mock_project_id):
        service_mock = get_conn_mock.return_value

        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        mock_operation_get = (
            service_mock.projects.return_value.databases.return_value.operations.return_value.get
        )
        (mock_export_documents.return_value.execute.return_value) = TEST_OPERATION

        execute_mock = mock.Mock(**{"side_effect": [TEST_WAITING_OPERATION, TEST_ERROR_OPERATION]})
        mock_operation_get.return_value.execute = execute_mock
        with pytest.raises(AirflowException, match="error"):
            self.hook.export_documents(body=EXPORT_DOCUMENT_BODY)


class TestCloudFirestoreHookWithoutProjectId:
    hook: CloudFirestoreHook | None = None

    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudFirestoreHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    def test_create_build(self, mock_get_conn, mock_project_id):
        with pytest.raises(AirflowException) as ctx:
            self.hook.export_documents(body={})

        assert (
            str(ctx.value)
            == "The project id must be passed either as keyword project_id parameter or as project_id extra in "
            "Google Cloud connection definition. Both are not set!"
        )


class TestCloudFirestoreAsyncHook:
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreAsyncHook.get_sync_hook")
    async def test_get_operation(self, mock_get_sync_hook):
        sync_hook_mock = mock.MagicMock()
        sync_hook_mock.get_operation.return_value = TEST_DONE_OPERATION
        mock_get_sync_hook.return_value = sync_hook_mock

        async_hook = CloudFirestoreAsyncHook(
            api_version="v1",
            gcp_conn_id="google_cloud_default",
            impersonation_chain="sa@project.iam.gserviceaccount.com",
        )
        result = await async_hook.get_operation(operation_name="op-123")

        assert result == TEST_DONE_OPERATION
        sync_hook_mock.get_operation.assert_called_once_with(operation_name="op-123")
