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

from airflow import AirflowException
from airflow.gcp.hooks.functions import GcfHook
from tests.compat import PropertyMock, mock
from tests.gcp.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST, get_open_mock, mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

GCF_LOCATION = 'location'
GCF_FUNCTION = 'function'


class TestFunctionHookNoDefaultProjectId(unittest.TestCase):

    def setUp(self):
        with mock.patch('airflow.gcp.hooks.base.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_no_default_project_id):
            self.gcf_function_hook_no_project_id = GcfHook(gcp_conn_id='test', api_version='v1')

    @mock.patch("airflow.gcp.hooks.functions.GcfHook._authorize")
    @mock.patch("airflow.gcp.hooks.functions.build")
    def test_gcf_client_creation(self, mock_build, mock_authorize):
        result = self.gcf_function_hook_no_project_id.get_conn()
        mock_build.assert_called_once_with(
            'cloudfunctions', 'v1', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.gcf_function_hook_no_project_id._conn, result)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    @mock.patch('airflow.gcp.hooks.functions.GcfHook._wait_for_operation_to_complete')
    def test_create_new_function_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        create_method = get_conn.return_value.projects.return_value.locations. \
            return_value.functions.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.gcf_function_hook_no_project_id.create_new_function(
                location=GCF_LOCATION,
                body={}
            )
        create_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    @mock.patch('airflow.gcp.hooks.functions.GcfHook._wait_for_operation_to_complete')
    def test_create_new_function_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        create_method = get_conn.return_value.projects.return_value.locations. \
            return_value.functions.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gcf_function_hook_no_project_id.create_new_function(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            location=GCF_LOCATION,
            body={}
        )
        self.assertIsNone(res)
        create_method.assert_called_once_with(body={},
                                              location='projects/example-project/locations/location')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(operation_name='operation_id')

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('requests.put')
    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    def test_upload_function_zip_missing_project_id(
        self, get_conn, requests_put, mock_project_id
    ):
        mck = mock.mock_open()
        with mock.patch('builtins.open', mck):
            generate_upload_url_method = get_conn.return_value.projects.return_value.locations. \
                return_value.functions.return_value.generateUploadUrl
            execute_method = generate_upload_url_method.return_value.execute
            execute_method.return_value = {"uploadUrl": "http://uploadHere"}
            requests_put.return_value = None
            with self.assertRaises(AirflowException) as cm:
                self.gcf_function_hook_no_project_id.upload_function_zip(
                    location=GCF_LOCATION,
                    zip_path="/tmp/path.zip"
                )
                generate_upload_url_method.assert_not_called()
                execute_method.assert_not_called()
                mck.assert_not_called()
                err = cm.exception
                self.assertIn("The project id must be passed", str(err))

    @mock.patch('requests.put')
    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    def test_upload_function_zip_overridden_project_id(self, get_conn, requests_put):
        mck, open_module = get_open_mock()
        with mock.patch('{}.open'.format(open_module), mck):
            generate_upload_url_method = get_conn.return_value.projects.return_value.locations. \
                return_value.functions.return_value.generateUploadUrl
            execute_method = generate_upload_url_method.return_value.execute
            execute_method.return_value = {"uploadUrl": "http://uploadHere"}
            requests_put.return_value = None
            res = self.gcf_function_hook_no_project_id.upload_function_zip(
                project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
                location=GCF_LOCATION,
                zip_path="/tmp/path.zip"
            )
            self.assertEqual("http://uploadHere", res)
            generate_upload_url_method.assert_called_once_with(
                parent='projects/example-project/locations/location')
            execute_method.assert_called_once_with(num_retries=5)
            requests_put.assert_called_once_with(
                data=mock.ANY,
                headers={'Content-type': 'application/zip',
                         'x-goog-content-length-range': '0,104857600'},
                url='http://uploadHere'
            )


class TestFunctionHookDefaultProjectId(unittest.TestCase):
    def setUp(self):
        with mock.patch('airflow.gcp.hooks.base.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_default_project_id):
            self.gcf_function_hook = GcfHook(gcp_conn_id='test', api_version='v1')

    @mock.patch("airflow.gcp.hooks.functions.GcfHook._authorize")
    @mock.patch("airflow.gcp.hooks.functions.build")
    def test_gcf_client_creation(self, mock_build, mock_authorize):
        result = self.gcf_function_hook.get_conn()
        mock_build.assert_called_once_with(
            'cloudfunctions', 'v1', http=mock_authorize.return_value, cache_discovery=False
        )
        self.assertEqual(mock_build.return_value, result)
        self.assertEqual(self.gcf_function_hook._conn, result)

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    @mock.patch('airflow.gcp.hooks.functions.GcfHook._wait_for_operation_to_complete')
    def test_create_new_function(self, wait_for_operation_to_complete, get_conn, mock_project_id):
        create_method = get_conn.return_value.projects.return_value.locations.\
            return_value.functions.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gcf_function_hook.create_new_function(
            location=GCF_LOCATION,
            body={}
        )
        self.assertIsNone(res)
        create_method.assert_called_once_with(body={},
                                              location='projects/example-project/locations/location')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(operation_name='operation_id')

    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    @mock.patch('airflow.gcp.hooks.functions.GcfHook._wait_for_operation_to_complete')
    def test_create_new_function_override_project_id(self, wait_for_operation_to_complete, get_conn):
        create_method = get_conn.return_value.projects.return_value.locations. \
            return_value.functions.return_value.create
        execute_method = create_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gcf_function_hook.create_new_function(
            project_id='new-project',
            location=GCF_LOCATION,
            body={}
        )
        self.assertIsNone(res)
        create_method.assert_called_once_with(body={},
                                              location='projects/new-project/locations/location')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(operation_name='operation_id')

    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    def test_get_function(self, get_conn):
        get_method = get_conn.return_value.projects.return_value.locations. \
            return_value.functions.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "function"}
        res = self.gcf_function_hook.get_function(
            name=GCF_FUNCTION
        )
        self.assertIsNotNone(res)
        self.assertEqual('function', res['name'])
        get_method.assert_called_once_with(name='function')
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    @mock.patch('airflow.gcp.hooks.functions.GcfHook._wait_for_operation_to_complete')
    def test_delete_function(self, wait_for_operation_to_complete, get_conn):
        delete_method = get_conn.return_value.projects.return_value.locations. \
            return_value.functions.return_value.delete
        execute_method = delete_method.return_value.execute
        wait_for_operation_to_complete.return_value = None
        execute_method.return_value = {"name": "operation_id"}
        res = self.gcf_function_hook.delete_function(  # pylint: disable=assignment-from-no-return
            name=GCF_FUNCTION
        )
        self.assertIsNone(res)
        delete_method.assert_called_once_with(name='function')
        execute_method.assert_called_once_with(num_retries=5)

    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    @mock.patch('airflow.gcp.hooks.functions.GcfHook._wait_for_operation_to_complete')
    def test_update_function(self, wait_for_operation_to_complete, get_conn):
        patch_method = get_conn.return_value.projects.return_value.locations. \
            return_value.functions.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.gcf_function_hook.update_function(  # pylint: disable=assignment-from-no-return
            update_mask=['a', 'b', 'c'],
            name=GCF_FUNCTION,
            body={}
        )
        self.assertIsNone(res)
        patch_method.assert_called_once_with(
            body={},
            name='function',
            updateMask='a,b,c'
        )
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(operation_name='operation_id')

    @mock.patch(
        'airflow.gcp.hooks.base.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('requests.put')
    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    def test_upload_function_zip(self, get_conn, requests_put, mock_project_id):
        mck, open_module = get_open_mock()
        with mock.patch('{}.open'.format(open_module), mck):
            generate_upload_url_method = get_conn.return_value.projects.return_value.locations. \
                return_value.functions.return_value.generateUploadUrl
            execute_method = generate_upload_url_method.return_value.execute
            execute_method.return_value = {"uploadUrl": "http://uploadHere"}
            requests_put.return_value = None
            res = self.gcf_function_hook.upload_function_zip(
                location=GCF_LOCATION,
                zip_path="/tmp/path.zip"
            )
            self.assertEqual("http://uploadHere", res)
            generate_upload_url_method.assert_called_once_with(
                parent='projects/example-project/locations/location')
            execute_method.assert_called_once_with(num_retries=5)
            requests_put.assert_called_once_with(
                data=mock.ANY,
                headers={'Content-type': 'application/zip',
                         'x-goog-content-length-range': '0,104857600'},
                url='http://uploadHere'
            )

    @mock.patch('requests.put')
    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    def test_upload_function_zip_overridden_project_id(self, get_conn, requests_put):
        mck, open_module = get_open_mock()
        with mock.patch('{}.open'.format(open_module), mck):
            generate_upload_url_method = get_conn.return_value.projects.return_value.locations. \
                return_value.functions.return_value.generateUploadUrl
            execute_method = generate_upload_url_method.return_value.execute
            execute_method.return_value = {"uploadUrl": "http://uploadHere"}
            requests_put.return_value = None
            res = self.gcf_function_hook.upload_function_zip(
                project_id='new-project',
                location=GCF_LOCATION,
                zip_path="/tmp/path.zip"
            )
            self.assertEqual("http://uploadHere", res)
            generate_upload_url_method.assert_called_once_with(
                parent='projects/new-project/locations/location')
            execute_method.assert_called_once_with(num_retries=5)
            requests_put.assert_called_once_with(
                data=mock.ANY,
                headers={'Content-type': 'application/zip',
                         'x-goog-content-length-range': '0,104857600'},
                url='http://uploadHere'
            )

    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    def test_call_function(self, mock_get_conn):
        payload = {'executionId': 'wh41ppcyoa6l', 'result': 'Hello World!'}
        call = mock_get_conn.return_value.projects.return_value.\
            locations.return_value.functions.return_value.call
        call.return_value.execute.return_value = payload

        function_id = "function1234"
        input_data = {'key': 'value'}
        name = "projects/{project_id}/locations/{location}/functions/{function_id}".format(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            location=GCF_LOCATION,
            function_id=function_id
        )

        result = self.gcf_function_hook.call_function(
            function_id=function_id,
            location=GCF_LOCATION,
            input_data=input_data,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST
        )

        call.assert_called_once_with(body=input_data, name=name)
        self.assertDictEqual(result, payload)

    @mock.patch('airflow.gcp.hooks.functions.GcfHook.get_conn')
    def test_call_function_error(self, mock_get_conn):
        payload = {'error': 'Something very bad'}
        call = mock_get_conn.return_value.projects.return_value. \
            locations.return_value.functions.return_value.call
        call.return_value.execute.return_value = payload

        function_id = "function1234"
        input_data = {'key': 'value'}
        with self.assertRaises(AirflowException):
            self.gcf_function_hook.call_function(
                function_id=function_id,
                location=GCF_LOCATION,
                input_data=input_data,
                project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST
            )
