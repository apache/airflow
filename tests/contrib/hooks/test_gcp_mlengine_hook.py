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

import json
import unittest

from unittest import mock
import requests
from google.auth.exceptions import GoogleAuthError
from googleapiclient.discovery import build_from_document
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpMockSequence
from urllib.parse import urlparse, parse_qsl

from airflow.contrib.hooks import gcp_mlengine_hook as hook

cml_available = True
try:
    hook.MLEngineHook().get_conn()
except GoogleAuthError:
    cml_available = False


class _TestMLEngineHook:

    def __init__(self, test_cls, responses, expected_requests):
        """
        Init method.

        Usage example:
        with _TestMLEngineHook(self, responses, expected_requests) as hook:
            self.run_my_test(hook)

        Args:
          test_cls: The caller's instance used for test communication.
          responses: A list of (dict_response, response_content) tuples.
          expected_requests: A list of (uri, http_method, body) tuples.
        """

        self._test_cls = test_cls
        self._responses = responses
        self._expected_requests = [
            self._normalize_requests_for_comparison(x[0], x[1], x[2])
            for x in expected_requests]
        self._actual_requests = []

    @staticmethod
    def _normalize_requests_for_comparison(uri, http_method, body):
        parts = urlparse(uri)
        return (
            parts._replace(query=set(parse_qsl(parts.query))),
            http_method,
            body)

    def __enter__(self):
        http = HttpMockSequence(self._responses)
        native_request_method = http.request

        # Collecting requests to validate at __exit__.
        def _request_wrapper(*args, **kwargs):
            self._actual_requests.append(args + (kwargs.get('body', ''),))
            return native_request_method(*args, **kwargs)

        http.request = _request_wrapper
        discovery = requests.get(
            'https://www.googleapis.com/discovery/v1/apis/ml/v1/rest')
        service_mock = build_from_document(discovery.json(), http=http)
        with mock.patch.object(
                hook.MLEngineHook, 'get_conn', return_value=service_mock):
            return hook.MLEngineHook()

    def __exit__(self, *args):
        # Propogating exceptions here since assert will silence them.
        if any(args):
            return None
        self._test_cls.assertEqual(
            [self._normalize_requests_for_comparison(x[0], x[1], x[2])
                for x in self._actual_requests],
            self._expected_requests)


class TestMLEngineHook(unittest.TestCase):

    def setUp(self):
        pass

    _SKIP_IF = unittest.skipIf(not cml_available,
                               'MLEngine is not available to run tests')

    _SERVICE_URI_PREFIX = 'https://ml.googleapis.com/v1/'

    @_SKIP_IF
    def test_create_version(self):
        project = 'test-project'
        model_name = 'test-model'
        version = 'test-version'
        operation_name = 'projects/{}/operations/test-operation'.format(
            project)

        response_body = {'name': operation_name, 'done': True}
        succeeded_response = ({'status': '200'}, json.dumps(response_body))

        expected_requests = [
            ('{}projects/{}/models/{}/versions?alt=json'.format(
                self._SERVICE_URI_PREFIX, project, model_name), 'POST',
             '"{}"'.format(version)),
            ('{}{}?alt=json'.format(self._SERVICE_URI_PREFIX, operation_name),
             'GET', None),
        ]

        with _TestMLEngineHook(
                self,
                responses=[succeeded_response] * 2,
                expected_requests=expected_requests) as cml_hook:
            create_version_response = cml_hook.create_version(
                project_id=project, model_name=model_name,
                version_spec=version)
            self.assertEqual(create_version_response, response_body)

    @_SKIP_IF
    def test_set_default_version(self):
        project = 'test-project'
        model_name = 'test-model'
        version = 'test-version'
        operation_name = 'projects/{}/operations/test-operation'.format(
            project)

        response_body = {'name': operation_name, 'done': True}
        succeeded_response = ({'status': '200'}, json.dumps(response_body))

        expected_requests = [
            ('{}projects/{}/models/{}/versions/{}:setDefault?alt=json'.format(
                self._SERVICE_URI_PREFIX, project, model_name, version),
                'POST', '{}'),
        ]

        with _TestMLEngineHook(
                self,
                responses=[succeeded_response],
                expected_requests=expected_requests) as cml_hook:
            set_default_version_response = cml_hook.set_default_version(
                project_id=project, model_name=model_name,
                version_name=version)
            self.assertEqual(set_default_version_response, response_body)

    @_SKIP_IF
    def test_list_versions(self):
        project = 'test-project'
        model_name = 'test-model'
        operation_name = 'projects/{}/operations/test-operation'.format(
            project)

        # This test returns the versions one at a time.
        versions = ['ver_{}'.format(ix) for ix in range(3)]

        response_bodies = [
            {
                'name': operation_name,
                'nextPageToken': ix,
                'versions': [ver]
            } for ix, ver in enumerate(versions)]
        response_bodies[-1].pop('nextPageToken')
        responses = [({'status': '200'}, json.dumps(body))
                     for body in response_bodies]

        expected_requests = [
            ('{}projects/{}/models/{}/versions?alt=json&pageSize=100'.format(
                self._SERVICE_URI_PREFIX, project, model_name), 'GET',
             None),
        ] + [
            ('{}projects/{}/models/{}/versions?alt=json&pageToken={}&pageSize=100'.format(
                self._SERVICE_URI_PREFIX, project, model_name, ix), 'GET',
             None) for ix in range(len(versions) - 1)
        ]

        with _TestMLEngineHook(
                self,
                responses=responses,
                expected_requests=expected_requests) as cml_hook:
            list_versions_response = cml_hook.list_versions(
                project_id=project, model_name=model_name)
            self.assertEqual(list_versions_response, versions)

    @_SKIP_IF
    def test_delete_version(self):
        project = 'test-project'
        model_name = 'test-model'
        version = 'test-version'
        operation_name = 'projects/{}/operations/test-operation'.format(
            project)

        not_done_response_body = {'name': operation_name, 'done': False}
        done_response_body = {'name': operation_name, 'done': True}
        not_done_response = (
            {'status': '200'}, json.dumps(not_done_response_body))
        succeeded_response = (
            {'status': '200'}, json.dumps(done_response_body))

        expected_requests = [
            (
                '{}projects/{}/models/{}/versions/{}?alt=json'.format(
                    self._SERVICE_URI_PREFIX, project, model_name, version),
                'DELETE',
                None),
            ('{}{}?alt=json'.format(self._SERVICE_URI_PREFIX, operation_name),
             'GET', None),
        ]

        with _TestMLEngineHook(
                self,
                responses=[not_done_response, succeeded_response],
                expected_requests=expected_requests) as cml_hook:
            delete_version_response = cml_hook.delete_version(
                project_id=project, model_name=model_name,
                version_name=version)
            self.assertEqual(delete_version_response, done_response_body)

    @_SKIP_IF
    def test_create_model(self):
        project = 'test-project'
        model_name = 'test-model'
        model = {
            'name': model_name,
        }
        response_body = {}
        succeeded_response = ({'status': '200'}, json.dumps(response_body))

        expected_requests = [
            ('{}projects/{}/models?alt=json'.format(
                self._SERVICE_URI_PREFIX, project), 'POST',
             json.dumps(model)),
        ]

        with _TestMLEngineHook(
                self,
                responses=[succeeded_response],
                expected_requests=expected_requests) as cml_hook:
            create_model_response = cml_hook.create_model(
                project_id=project, model=model)
            self.assertEqual(create_model_response, response_body)

    @_SKIP_IF
    def test_get_model(self):
        project = 'test-project'
        model_name = 'test-model'
        response_body = {'model': model_name}
        succeeded_response = ({'status': '200'}, json.dumps(response_body))

        expected_requests = [
            ('{}projects/{}/models/{}?alt=json'.format(
                self._SERVICE_URI_PREFIX, project, model_name), 'GET',
             None),
        ]

        with _TestMLEngineHook(
                self,
                responses=[succeeded_response],
                expected_requests=expected_requests) as cml_hook:
            get_model_response = cml_hook.get_model(
                project_id=project, model_name=model_name)
            self.assertEqual(get_model_response, response_body)

    @_SKIP_IF
    def test_create_mlengine_job(self):
        project = 'test-project'
        job_id = 'test-job-id'
        my_job = {
            'jobId': job_id,
            'foo': 4815162342,
            'state': 'SUCCEEDED',
        }
        response_body = json.dumps(my_job)
        succeeded_response = ({'status': '200'}, response_body)
        queued_response = ({'status': '200'}, json.dumps({
            'jobId': job_id,
            'state': 'QUEUED',
        }))

        create_job_request = ('{}projects/{}/jobs?alt=json'.format(
            self._SERVICE_URI_PREFIX, project), 'POST', response_body)
        ask_if_done_request = ('{}projects/{}/jobs/{}?alt=json'.format(
            self._SERVICE_URI_PREFIX, project, job_id), 'GET', None)
        expected_requests = [
            create_job_request,
            ask_if_done_request,
            ask_if_done_request,
        ]
        responses = [succeeded_response,
                     queued_response, succeeded_response]

        with _TestMLEngineHook(
                self,
                responses=responses,
                expected_requests=expected_requests) as cml_hook:
            create_job_response = cml_hook.create_job(
                project_id=project, job=my_job)
            self.assertEqual(create_job_response, my_job)

    @_SKIP_IF
    def test_create_mlengine_job_reuse_existing_job_by_default(self):
        project = 'test-project'
        job_id = 'test-job-id'
        my_job = {
            'jobId': job_id,
            'foo': 4815162342,
            'state': 'SUCCEEDED',
        }
        response_body = json.dumps(my_job)
        job_already_exist_response = ({'status': '409'}, json.dumps({}))
        succeeded_response = ({'status': '200'}, response_body)

        create_job_request = ('{}projects/{}/jobs?alt=json'.format(
            self._SERVICE_URI_PREFIX, project), 'POST', response_body)
        ask_if_done_request = ('{}projects/{}/jobs/{}?alt=json'.format(
            self._SERVICE_URI_PREFIX, project, job_id), 'GET', None)
        expected_requests = [
            create_job_request,
            ask_if_done_request,
        ]
        responses = [job_already_exist_response, succeeded_response]

        # By default, 'create_job' reuse the existing job.
        with _TestMLEngineHook(
                self,
                responses=responses,
                expected_requests=expected_requests) as cml_hook:
            create_job_response = cml_hook.create_job(
                project_id=project, job=my_job)
            self.assertEqual(create_job_response, my_job)

    @_SKIP_IF
    def test_create_mlengine_job_check_existing_job(self):
        project = 'test-project'
        job_id = 'test-job-id'
        my_job = {
            'jobId': job_id,
            'foo': 4815162342,
            'state': 'SUCCEEDED',
            'someInput': {
                'input': 'someInput'
            }
        }
        different_job = {
            'jobId': job_id,
            'foo': 4815162342,
            'state': 'SUCCEEDED',
            'someInput': {
                'input': 'someDifferentInput'
            }
        }

        my_job_response_body = json.dumps(my_job)
        different_job_response_body = json.dumps(different_job)
        job_already_exist_response = ({'status': '409'}, json.dumps({}))
        different_job_response = ({'status': '200'},
                                  different_job_response_body)

        create_job_request = ('{}projects/{}/jobs?alt=json'.format(
            self._SERVICE_URI_PREFIX, project), 'POST', my_job_response_body)
        ask_if_done_request = ('{}projects/{}/jobs/{}?alt=json'.format(
            self._SERVICE_URI_PREFIX, project, job_id), 'GET', None)
        expected_requests = [
            create_job_request,
            ask_if_done_request,
        ]

        # Returns a different job (with different 'someInput' field) will
        # cause 'create_job' request to fail.
        responses = [job_already_exist_response, different_job_response]

        def check_input(existing_job):
            return existing_job.get('someInput', None) == \
                my_job['someInput']
        with _TestMLEngineHook(
                self,
                responses=responses,
                expected_requests=expected_requests) as cml_hook:
            with self.assertRaises(HttpError):
                cml_hook.create_job(
                    project_id=project, job=my_job,
                    use_existing_job_fn=check_input)

        my_job_response = ({'status': '200'}, my_job_response_body)
        expected_requests = [
            create_job_request,
            ask_if_done_request,
            ask_if_done_request,
        ]
        responses = [
            job_already_exist_response,
            my_job_response,
            my_job_response]
        with _TestMLEngineHook(
                self,
                responses=responses,
                expected_requests=expected_requests) as cml_hook:
            create_job_response = cml_hook.create_job(
                project_id=project, job=my_job,
                use_existing_job_fn=check_input)
            self.assertEqual(create_job_response, my_job)


if __name__ == '__main__':
    unittest.main()
