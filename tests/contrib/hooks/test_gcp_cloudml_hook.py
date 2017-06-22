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
try:  # python 2
    from urlparse import urlparse, parse_qsl
except ImportError:  # python 3
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
        """Init method.

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
        return (
            parts._replace(query=set(parse_qsl(parts.query))),
            http_method,
            body)

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
            [self._normalize_requests_for_comparison(x[0], x[1], x[2]) for x in self._actual_requests],
            self._expected_requests)


class TestCloudMLHook(unittest.TestCase):

    def setUp(self):
        pass

    _SKIP_IF = unittest.skipIf(not cml_available,
                               'CloudML is not available to run tests')

    _SERVICE_URI_PREFIX = 'https://ml.googleapis.com/v1/'

    @_SKIP_IF
    def test_create_cloudml_job(self):
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

        with _TestCloudMLHook(
                self,
                responses=responses,
                expected_requests=expected_requests) as cml_hook:
            create_job_response = cml_hook.create_job(
                project_name=project, job=my_job)
            self.assertEquals(create_job_response, my_job)


if __name__ == '__main__':
    unittest.main()
