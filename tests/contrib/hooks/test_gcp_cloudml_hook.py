# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import mock
import unittest
try: # python 2
    from urlparse import urlparse, parse_qsl
except ImportError: #python 3
    from urllib.parse import urlparse, parse_qsl

from airflow.contrib.hooks import gcp_cloudml_hook as hook
from apiclient.discovery import build
from apiclient.http import HttpMockSequence
from oauth2client.contrib.gce import HttpAccessTokenRefreshError

cml_available = True
try:
    hook.CloudMLHook().get_conn()
except HttpAccessTokenRefreshError:
    cml_available = False


class _TestCloudMLHook(object):

    def __init__(self, test_cls, responses, expected_requests):
        """
        Init method.

        Usage example:
        with _TestCloudMLHook(self, responses, expected_requests) as hook:
            self.run_my_test(hook)

        Args:
          test_cls: The caller's instance used for test communication.
          responses: A list of (dict_response, response_content) tuples.
          expected_requests: A list of (uri, http_method, body) tuples.
        """

        self._test_cls = test_cls
        self._responses = responses
        self._expected_requests = [
            self._normalize_requests_for_comparison(x[0], x[1], x[2]) for x in expected_requests]
        self._actual_requests = []

    def _normalize_requests_for_comparison(self, uri, http_method, body):
        parts = urlparse(uri)
        return (parts._replace(query=set(parse_qsl(parts.query))), http_method, body)

    def __enter__(self):
        http = HttpMockSequence(self._responses)
        native_request_method = http.request

        # Collecting requests to validate at __exit__.
        def _request_wrapper(*args, **kwargs):
            self._actual_requests.append(args + (kwargs['body'],))
            return native_request_method(*args, **kwargs)

        http.request = _request_wrapper
        service_mock = build('ml', 'v1', http=http)
        with mock.patch.object(
                hook.CloudMLHook, 'get_conn', return_value=service_mock):
            return hook.CloudMLHook()

    def __exit__(self, *args):
        # Propogating exceptions here since assert will silence them.
        if any(args):
            return None
        self._test_cls.assertEquals(
            [self._normalize_requests_for_comparison(x[0], x[1], x[2]) for x in self._actual_requests], self._expected_requests)


class TestCloudMLHook(unittest.TestCase):

    def setUp(self):
        pass

    _SKIP_IF = unittest.skipIf(not cml_available,
                               'CloudML is not available to run tests')
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

        with _TestCloudMLHook(
                self,
                responses=[succeeded_response] * 2,
                expected_requests=expected_requests) as cml_hook:
            create_version_response = cml_hook.create_version(
                project_name=project, model_name=model_name, version_spec=version)
            self.assertEquals(create_version_response, response_body)

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
                self._SERVICE_URI_PREFIX, project, model_name, version), 'POST',
             '{}'),
        ]

        with _TestCloudMLHook(
                self,
                responses=[succeeded_response],
                expected_requests=expected_requests) as cml_hook:
            set_default_version_response = cml_hook.set_default_version(
                project_name=project, model_name=model_name, version_name=version)
            self.assertEquals(set_default_version_response, response_body)

    @_SKIP_IF
    def test_list_versions(self):
        project = 'test-project'
        model_name = 'test-model'
        operation_name = 'projects/{}/operations/test-operation'.format(
            project)

        # This test returns the versions one at a time.
        versions = ['ver_{}'.format(ix) for ix in range(3)]

        response_bodies = [{'name': operation_name, 'nextPageToken': ix, 'versions': [
            ver]} for ix, ver in enumerate(versions)]
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

        with _TestCloudMLHook(
                self,
                responses=responses,
                expected_requests=expected_requests) as cml_hook:
            list_versions_response = cml_hook.list_versions(
                project_name=project, model_name=model_name)
            self.assertEquals(list_versions_response, versions)

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
            ('{}projects/{}/models/{}/versions/{}?alt=json'.format(
                self._SERVICE_URI_PREFIX, project, model_name, version), 'DELETE',
             None),
            ('{}{}?alt=json'.format(self._SERVICE_URI_PREFIX, operation_name),
             'GET', None),
        ]

        with _TestCloudMLHook(
                self,
                responses=[not_done_response, succeeded_response],
                expected_requests=expected_requests) as cml_hook:
            delete_version_response = cml_hook.delete_version(
                project_name=project, model_name=model_name, version_name=version)
            self.assertEquals(delete_version_response, done_response_body)

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

        with _TestCloudMLHook(
                self,
                responses=[succeeded_response],
                expected_requests=expected_requests) as cml_hook:
            create_model_response = cml_hook.create_model(
                project_name=project, model=model)
            self.assertEquals(create_model_response, response_body)

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

        with _TestCloudMLHook(
                self,
                responses=[succeeded_response],
                expected_requests=expected_requests) as cml_hook:
            get_model_response = cml_hook.get_model(
                project_name=project, model_name=model_name)
            self.assertEquals(get_model_response, response_body)


if __name__ == '__main__':
    unittest.main()
