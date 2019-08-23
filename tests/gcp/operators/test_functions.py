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

from copy import deepcopy

from googleapiclient.errors import HttpError
from parameterized import parameterized

from airflow.gcp.operators.functions import \
    GcfFunctionDeployOperator, GcfFunctionDeleteOperator, FUNCTION_NAME_PATTERN
from airflow import AirflowException
from airflow.version import version
from tests.compat import mock


EMPTY_CONTENT = b''
MOCK_RESP_404 = type('', (object,), {"status": 404})()

GCP_PROJECT_ID = 'test_project_id'
GCP_LOCATION = 'test_region'
GCF_SOURCE_ARCHIVE_URL = 'gs://folder/file.zip'
GCF_ENTRYPOINT = 'helloWorld'
FUNCTION_NAME = 'projects/{}/locations/{}/functions/{}'.format(GCP_PROJECT_ID,
                                                               GCP_LOCATION,
                                                               GCF_ENTRYPOINT)
GCF_RUNTIME = 'nodejs6'
VALID_RUNTIMES = ['nodejs6', 'nodejs8', 'python37']
VALID_BODY = {
    "name": FUNCTION_NAME,
    "entryPoint": GCF_ENTRYPOINT,
    "runtime": GCF_RUNTIME,
    "httpsTrigger": {},
    "sourceArchiveUrl": GCF_SOURCE_ARCHIVE_URL
}


def _prepare_test_bodies():
    body_no_name = deepcopy(VALID_BODY)
    body_no_name.pop('name', None)
    body_empty_entry_point = deepcopy(VALID_BODY)
    body_empty_entry_point['entryPoint'] = ''
    body_empty_runtime = deepcopy(VALID_BODY)
    body_empty_runtime['runtime'] = ''
    body_values = [
        ({}, "The required parameter 'body' is missing"),
        (body_no_name, "The required body field 'name' is missing"),
        (body_empty_entry_point,
         "The body field 'entryPoint' of value '' does not match"),
        (body_empty_runtime, "The body field 'runtime' of value '' does not match"),
    ]
    return body_values


class TestGcfFunctionDeploy(unittest.TestCase):
    @parameterized.expand(_prepare_test_bodies())
    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_body_empty_or_missing_fields(self, body, message, mock_hook):
        mock_hook.return_value.upload_function_zip.return_value = 'https://uploadUrl'
        with self.assertRaises(AirflowException) as cm:
            op = GcfFunctionDeployOperator(
                project_id="test_project_id",
                location="test_region",
                body=body,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn(message, str(err))

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_deploy_execute(self, mock_hook):
        mock_hook.return_value.get_function.side_effect = mock.Mock(
            side_effect=HttpError(resp=MOCK_RESP_404, content=b'not found'))
        mock_hook.return_value.create_new_function.return_value = True
        op = GcfFunctionDeployOperator(
            project_id=GCP_PROJECT_ID,
            location=GCP_LOCATION,
            body=deepcopy(VALID_BODY),
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.get_function.assert_called_once_with(
            'projects/test_project_id/locations/test_region/functions/helloWorld'
        )
        expected_body = deepcopy(VALID_BODY)
        expected_body['labels'] = {
            'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')
        }
        mock_hook.return_value.create_new_function.assert_called_once_with(
            project_id='test_project_id',
            location='test_region',
            body=expected_body
        )

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_update_function_if_exists(self, mock_hook):
        mock_hook.return_value.get_function.return_value = True
        mock_hook.return_value.update_function.return_value = True
        op = GcfFunctionDeployOperator(
            project_id=GCP_PROJECT_ID,
            location=GCP_LOCATION,
            body=deepcopy(VALID_BODY),
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.get_function.assert_called_once_with(
            'projects/test_project_id/locations/test_region/functions/helloWorld'
        )
        expected_body = deepcopy(VALID_BODY)
        expected_body['labels'] = {
            'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')
        }
        mock_hook.return_value.update_function.assert_called_once_with(
            'projects/test_project_id/locations/test_region/functions/helloWorld',
            expected_body, expected_body.keys())
        mock_hook.return_value.create_new_function.assert_not_called()

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_empty_project_id_is_ok(self, mock_hook):
        mock_hook.return_value.get_function.side_effect = \
            HttpError(resp=MOCK_RESP_404, content=b'not found')
        operator = GcfFunctionDeployOperator(
            location="test_region",
            body=deepcopy(VALID_BODY),
            task_id="id"
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        new_body = deepcopy(VALID_BODY)
        new_body['labels'] = {
            'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')}
        mock_hook.return_value.create_new_function.assert_called_once_with(
            project_id=None,
            location="test_region",
            body=new_body)

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_empty_location(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            GcfFunctionDeployOperator(
                project_id="test_project_id",
                location="",
                body=None,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter 'location' is missing", str(err))

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_empty_body(self, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            GcfFunctionDeployOperator(
                project_id="test_project_id",
                location="test_region",
                body=None,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter 'body' is missing", str(err))

    @parameterized.expand([
        (runtime,) for runtime in VALID_RUNTIMES
    ])
    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_correct_runtime_field(self, runtime, mock_hook):
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body['runtime'] = runtime
        op = GcfFunctionDeployOperator(
            project_id="test_project_id",
            location="test_region",
            body=body,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        (network,) for network in [
            "network-01",
            "n-0-2-3-4",
            "projects/PROJECT/global/networks/network-01"
            "projects/PRÓJECT/global/networks/netwórk-01"
        ]
    ])
    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_valid_network_field(self, network, mock_hook):
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body['network'] = network
        op = GcfFunctionDeployOperator(
            project_id="test_project_id",
            location="test_region",
            body=body,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        (labels,) for labels in [
            {},
            {"label": 'value-01'},
            {"label_324234_a_b_c": 'value-01_93'},
        ]
    ])
    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_valid_labels_field(self, labels, mock_hook):
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body['labels'] = labels
        op = GcfFunctionDeployOperator(
            project_id="test_project_id",
            location="test_region",
            body=body,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_validation_disabled(self, mock_hook):
        mock_hook.return_value.create_new_function.return_value = True
        body = {
            "name": "function_name",
            "some_invalid_body_field": "some_invalid_body_field_value"
        }
        op = GcfFunctionDeployOperator(
            project_id="test_project_id",
            location="test_region",
            body=body,
            validate_body=False,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_body_validation_simple(self, mock_hook):
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body['name'] = ''
        with self.assertRaises(AirflowException) as cm:
            op = GcfFunctionDeployOperator(
                project_id="test_project_id",
                location="test_region",
                body=body,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The body field 'name' of value '' does not match",
                      str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        ('name', '',
         "The body field 'name' of value '' does not match"),
        ('description', '', "The body field 'description' of value '' does not match"),
        ('entryPoint', '', "The body field 'entryPoint' of value '' does not match"),
        ('availableMemoryMb', '0',
         "The available memory has to be greater than 0"),
        ('availableMemoryMb', '-1',
         "The available memory has to be greater than 0"),
        ('availableMemoryMb', 'ss',
         "invalid literal for int() with base 10: 'ss'"),
        ('network', '', "The body field 'network' of value '' does not match"),
        ('maxInstances', '0', "The max instances parameter has to be greater than 0"),
        ('maxInstances', '-1', "The max instances parameter has to be greater than 0"),
        ('maxInstances', 'ss', "invalid literal for int() with base 10: 'ss'"),
    ])
    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_invalid_field_values(self, key, value, message, mock_hook):
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body[key] = value
        with self.assertRaises(AirflowException) as cm:
            op = GcfFunctionDeployOperator(
                project_id="test_project_id",
                location="test_region",
                body=body,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn(message, str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        ({'sourceArchiveUrl': ''},
         "The body field 'source_code.sourceArchiveUrl' of value '' does not match"),
        ({'sourceArchiveUrl': '', 'zip_path': '/path/to/file'},
         "Only one of 'sourceArchiveUrl' in body or 'zip_path' argument allowed."),
        ({'sourceArchiveUrl': 'gs://url', 'zip_path': '/path/to/file'},
         "Only one of 'sourceArchiveUrl' in body or 'zip_path' argument allowed."),
        ({'sourceArchiveUrl': '', 'sourceUploadUrl': ''},
         "Parameter 'sourceUploadUrl' is empty in the body and argument "
         "'zip_path' is missing or empty."),
        ({'sourceArchiveUrl': 'gs://adasda', 'sourceRepository': ''},
         "The field 'source_code.sourceRepository' should be of dictionary type"),
        ({'sourceUploadUrl': '', 'sourceRepository': ''},
         "Parameter 'sourceUploadUrl' is empty in the body and argument 'zip_path' "
         "is missing or empty."),
        ({'sourceArchiveUrl': '', 'sourceUploadUrl': '', 'sourceRepository': ''},
         "Parameter 'sourceUploadUrl' is empty in the body and argument 'zip_path' "
         "is missing or empty."),
        ({'sourceArchiveUrl': 'gs://url', 'sourceUploadUrl': 'https://url'},
         "The mutually exclusive fields 'sourceUploadUrl' and 'sourceArchiveUrl' "
         "belonging to the union 'source_code' are both present. Please remove one"),
        ({'sourceUploadUrl': 'https://url', 'zip_path': '/path/to/file'},
         "Only one of 'sourceUploadUrl' in body "
         "or 'zip_path' argument allowed. Found both."),
        ({'sourceUploadUrl': ''}, "Parameter 'sourceUploadUrl' is empty in the body "
                                  "and argument 'zip_path' is missing or empty."),
        ({'sourceRepository': ''}, "The field 'source_code.sourceRepository' "
                                   "should be of dictionary type"),
        ({'sourceRepository': {}}, "The required body field "
                                   "'source_code.sourceRepository.url' is missing"),
        ({'sourceRepository': {'url': ''}},
         "The body field 'source_code.sourceRepository.url' of value '' does not match"),
    ]
    )
    def test_invalid_source_code_union_field(self, source_code, message):
        body = deepcopy(VALID_BODY)
        body.pop('sourceUploadUrl', None)
        body.pop('sourceArchiveUrl', None)
        zip_path = source_code.pop('zip_path', None)
        body.update(source_code)
        with self.assertRaises(AirflowException) as cm:
            op = GcfFunctionDeployOperator(
                project_id="test_project_id",
                location="test_region",
                body=body,
                task_id="id",
                zip_path=zip_path
            )
            op.execute(None)
        err = cm.exception
        self.assertIn(message, str(err))

    @parameterized.expand([
        ({'sourceArchiveUrl': 'gs://url'}, 'test_project_id'),
        ({'zip_path': '/path/to/file', 'sourceUploadUrl': None}, 'test_project_id'),
        ({'zip_path': '/path/to/file', 'sourceUploadUrl': None}, None),
        ({'sourceUploadUrl':
         'https://source.developers.google.com/projects/a/repos/b/revisions/c/paths/d'},
         'test_project_id'),
        ({'sourceRepository':
         {'url': 'https://source.developers.google.com/projects/a/'
          'repos/b/revisions/c/paths/d'}},
         'test_project_id'),
    ])
    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_valid_source_code_union_field(self, source_code, project_id, mock_hook):
        mock_hook.return_value.upload_function_zip.return_value = 'https://uploadUrl'
        mock_hook.return_value.get_function.side_effect = mock.Mock(
            side_effect=HttpError(resp=MOCK_RESP_404, content=b'not found'))
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body.pop('sourceUploadUrl', None)
        body.pop('sourceArchiveUrl', None)
        body.pop('sourceRepository', None)
        body.pop('sourceRepositoryUrl', None)
        zip_path = source_code.pop('zip_path', None)
        body.update(source_code)
        if project_id:
            op = GcfFunctionDeployOperator(
                project_id="test_project_id",
                location="test_region",
                body=body,
                task_id="id",
                zip_path=zip_path
            )
        else:
            op = GcfFunctionDeployOperator(
                location="test_region",
                body=body,
                task_id="id",
                zip_path=zip_path
            )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        if zip_path:
            mock_hook.return_value.upload_function_zip.assert_called_once_with(
                project_id=project_id,
                location='test_region',
                zip_path='/path/to/file'
            )
        mock_hook.return_value.get_function.assert_called_once_with(
            'projects/test_project_id/locations/test_region/functions/helloWorld'
        )
        mock_hook.return_value.create_new_function.assert_called_once_with(
            project_id=project_id,
            location='test_region',
            body=body
        )
        mock_hook.reset_mock()

    @parameterized.expand([
        ({'eventTrigger': {}},
         "The required body field 'trigger.eventTrigger.eventType' is missing"),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b'}},
         "The required body field 'trigger.eventTrigger.resource' is missing"),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b', 'resource': ''}},
         "The body field 'trigger.eventTrigger.resource' of value '' does not match"),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b',
                           'resource': 'res',
                           'service': ''}},
         "The body field 'trigger.eventTrigger.service' of value '' does not match"),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b',
                           'resource': 'res',
                           'service': 'service_name',
                           'failurePolicy': {'retry': ''}}},
         "The field 'trigger.eventTrigger.failurePolicy.retry' "
         "should be of dictionary type")
    ]
    )
    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_invalid_trigger_union_field(self, trigger, message, mock_hook):
        mock_hook.return_value.upload_function_zip.return_value = 'https://uploadUrl'
        body = deepcopy(VALID_BODY)
        body.pop('httpsTrigger', None)
        body.pop('eventTrigger', None)
        body.update(trigger)
        with self.assertRaises(AirflowException) as cm:
            op = GcfFunctionDeployOperator(
                project_id="test_project_id",
                location="test_region",
                body=body,
                task_id="id",
            )
            op.execute(None)
        err = cm.exception
        self.assertIn(message, str(err))
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()

    @parameterized.expand([
        ({'httpsTrigger': {}},),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b',
                           'resource': 'res'}},),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b',
                           'resource': 'res',
                           'service': 'service_name'}},),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/ą.b',
                           'resource': 'reś',
                           'service': 'service_namę'}},),
        ({'eventTrigger': {'eventType': 'providers/test/eventTypes/a.b',
                           'resource': 'res',
                           'service': 'service_name',
                           'failurePolicy': {'retry': {}}}},)
    ])
    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_valid_trigger_union_field(self, trigger, mock_hook):
        mock_hook.return_value.upload_function_zip.return_value = 'https://uploadUrl'
        mock_hook.return_value.get_function.side_effect = mock.Mock(
            side_effect=HttpError(resp=MOCK_RESP_404, content=b'not found'))
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body.pop('httpsTrigger', None)
        body.pop('eventTrigger', None)
        body.update(trigger)
        op = GcfFunctionDeployOperator(
            project_id="test_project_id",
            location="test_region",
            body=body,
            task_id="id",
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.get_function.assert_called_once_with(
            'projects/test_project_id/locations/test_region/functions/helloWorld'
        )
        mock_hook.return_value.create_new_function.assert_called_once_with(
            project_id='test_project_id',
            location='test_region',
            body=body
        )
        mock_hook.reset_mock()

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_extra_parameter(self, mock_hook):
        mock_hook.return_value.create_new_function.return_value = True
        body = deepcopy(VALID_BODY)
        body['extra_parameter'] = 'extra'
        op = GcfFunctionDeployOperator(
            project_id="test_project_id",
            location="test_region",
            body=body,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.reset_mock()


class TestGcfFunctionDelete(unittest.TestCase):
    _FUNCTION_NAME = 'projects/project_name/locations/project_location/functions' \
                     '/function_name'
    _DELETE_FUNCTION_EXPECTED = {
        '@type': 'type.googleapis.com/google.cloud.functions.v1.CloudFunction',
        'name': _FUNCTION_NAME,
        'sourceArchiveUrl': 'gs://functions/hello.zip',
        'httpsTrigger': {
            'url': 'https://project_location-project_name.cloudfunctions.net'
                   '/function_name'},
        'status': 'ACTIVE', 'entryPoint': 'entry_point', 'timeout': '60s',
        'availableMemoryMb': 256,
        'serviceAccountEmail': 'project_name@appspot.gserviceaccount.com',
        'updateTime': '2018-08-23T00:00:00Z',
        'versionId': '1', 'runtime': 'nodejs6'}

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_delete_execute(self, mock_hook):
        mock_hook.return_value.delete_function.return_value = \
            self._DELETE_FUNCTION_EXPECTED
        op = GcfFunctionDeleteOperator(
            name=self._FUNCTION_NAME,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.delete_function.assert_called_once_with(
            'projects/project_name/locations/project_location/functions/function_name'
        )
        self.assertEqual(result['name'], self._FUNCTION_NAME)

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_correct_name(self, mock_hook):
        op = GcfFunctionDeleteOperator(
            name="projects/project_name/locations/project_location/functions"
                 "/function_name",
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_invalid_name(self, mock_hook):
        with self.assertRaises(AttributeError) as cm:
            op = GcfFunctionDeleteOperator(
                name="invalid_name",
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertEqual(str(err), 'Parameter name must match pattern: {}'.format(
            FUNCTION_NAME_PATTERN))
        mock_hook.assert_not_called()

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_empty_name(self, mock_hook):
        mock_hook.return_value.delete_function.return_value = \
            self._DELETE_FUNCTION_EXPECTED
        with self.assertRaises(AttributeError) as cm:
            GcfFunctionDeleteOperator(
                name="",
                task_id="id"
            )
        err = cm.exception
        self.assertEqual(str(err), 'Empty parameter: name')
        mock_hook.assert_not_called()

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_gcf_error_silenced_when_function_doesnt_exist(self, mock_hook):
        op = GcfFunctionDeleteOperator(
            name=self._FUNCTION_NAME,
            task_id="id"
        )
        mock_hook.return_value.delete_function.side_effect = mock.Mock(
            side_effect=HttpError(resp=MOCK_RESP_404, content=b'not found'))
        op.execute(None)
        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.delete_function.assert_called_once_with(
            'projects/project_name/locations/project_location/functions/function_name'
        )

    @mock.patch('airflow.gcp.operators.functions.GcfHook')
    def test_non_404_gcf_error_bubbled_up(self, mock_hook):
        op = GcfFunctionDeleteOperator(
            name=self._FUNCTION_NAME,
            task_id="id"
        )
        resp = type('', (object,), {"status": 500})()
        mock_hook.return_value.delete_function.side_effect = mock.Mock(
            side_effect=HttpError(resp=resp, content=b'error'))

        with self.assertRaises(HttpError):
            op.execute(None)

        mock_hook.assert_called_once_with(api_version='v1',
                                          gcp_conn_id='google_cloud_default')
        mock_hook.return_value.delete_function.assert_called_once_with(
            'projects/project_name/locations/project_location/functions/function_name'
        )
