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
import unittest
from typing import Optional
from unittest import mock
from unittest.mock import PropertyMock

from airflow.exceptions import AirflowException
from airflow.providers.google.firebase.hooks.firestore import CloudFirestoreHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
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


class TestCloudFirestoreHookWithPassedProjectId(unittest.TestCase):
    hook = None  # type: Optional[CloudFirestoreHook]

    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudFirestoreHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook._authorize")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.build")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.build_from_document")
    def test_client_creation(self, mock_build_from_document, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with('firestore', 'v1', cache_discovery=False)
        mock_build_from_document.assert_called_once_with(
            mock_build.return_value._rootDesc, http=mock_authorize.return_value
        )
        self.assertEqual(mock_build_from_document.return_value, result)
        self.assertEqual(self.hook._conn, result)

    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    def test_mmediately_complete(self, get_conn_mock):
        service_mock = get_conn_mock.return_value

        mock_export_documents = service_mock.projects.return_value.databases.return_value.exportDocuments
        mock_operation_get = (
            service_mock.projects.return_value.databases.return_value.operations.return_value.get
        )
        (mock_export_documents.return_value.execute.return_value) = TEST_OPERATION

        (mock_operation_get.return_value.execute.return_value) = TEST_DONE_OPERATION

        self.hook.export_documents(body=EXPORT_DOCUMENT_BODY, project_id=TEST_PROJECT_ID)

        mock_export_documents.assert_called_once_with(
            body=EXPORT_DOCUMENT_BODY, name='projects/firestore--project-id/databases/(default)'
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
            body=EXPORT_DOCUMENT_BODY, name='projects/firestore--project-id/databases/(default)'
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
        with self.assertRaisesRegex(AirflowException, "error"):
            self.hook.export_documents(body=EXPORT_DOCUMENT_BODY, project_id=TEST_PROJECT_ID)


class TestCloudFirestoreHookWithDefaultProjectIdFromConnection(unittest.TestCase):
    hook = None  # type: Optional[CloudFirestoreHook]

    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudFirestoreHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook._authorize")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.build")
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.build_from_document")
    def test_client_creation(self, mock_build_from_document, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with('firestore', 'v1', cache_discovery=False)
        mock_build_from_document.assert_called_once_with(
            mock_build.return_value._rootDesc, http=mock_authorize.return_value
        )
        self.assertEqual(mock_build_from_document.return_value, result)
        self.assertEqual(self.hook._conn, result)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
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
            body=EXPORT_DOCUMENT_BODY, name='projects/example-project/databases/(default)'
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
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
            body=EXPORT_DOCUMENT_BODY, name='projects/example-project/databases/(default)'
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
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
        with self.assertRaisesRegex(AirflowException, "error"):
            self.hook.export_documents(body=EXPORT_DOCUMENT_BODY)


class TestCloudFirestoreHookWithoutProjectId(unittest.TestCase):
    hook = None  # type: Optional[CloudFirestoreHook]

    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudFirestoreHook(gcp_conn_id="test")

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None,
    )
    @mock.patch("airflow.providers.google.firebase.hooks.firestore.CloudFirestoreHook.get_conn")
    def test_create_build(self, mock_get_conn, mock_project_id):
        with self.assertRaises(AirflowException) as e:
            self.hook.export_documents(body={})

        self.assertEqual(
            "The project id must be passed either as keyword project_id parameter or as project_id extra in "
            "Google Cloud connection definition. Both are not set!",
            str(e.exception),
        )
