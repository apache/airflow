# -*- coding: utf-8 -*-
#
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
#

from mock import ANY, Mock, patch
import tempfile
import unittest

from oauth2client.service_account import ServiceAccountCredentials

from airflow.contrib.hooks import gcp_api_base_hook as hook


class TestGcpBaseHookOptions(unittest.TestCase):
    @patch(hook.__name__ + '.GoogleCloudBaseHook.__init__',
           Mock(return_value=None))
    @patch.object(ServiceAccountCredentials, 'from_json_keyfile_name')
    def test_base_hook_with_json_file(self, from_file):
        tmpfile = tempfile.NamedTemporaryFile(suffix='.json')
        gcp_hook = hook.GoogleCloudBaseHook('gcp_default')
        gcp_hook.extras = {
            'extra__google_cloud_platform__project': 'foo_project',
            'extra__google_cloud_platform__key_path': tmpfile.name,
            'extra__google_cloud_platform__scope': 's1,s2',
        }
        gcp_hook.delegate_to = None
        gcp_hook._authorize()
        from_file.assert_called_once_with(ANY, ANY)

    @patch(hook.__name__ + '.GoogleCloudBaseHook.__init__',
           Mock(return_value=None))
    @patch.object(ServiceAccountCredentials, 'from_json_keyfile_dict')
    def test_base_hook_with_json_dict(self, from_dict):
        gcp_hook = hook.GoogleCloudBaseHook('gcp_default')
        gcp_hook.extras = {
            'extra__google_cloud_platform__project': 'foo_project',
            'extra__google_cloud_platform__key_dict': dict(foo='bar'),
            'extra__google_cloud_platform__scope': 's1,s2',
        }
        gcp_hook.delegate_to = None
        gcp_hook._authorize()
        from_dict.assert_called_once_with(ANY, ANY)

    @patch(hook.__name__ + '.GoogleCloudBaseHook.__init__',
           Mock(return_value=None))
    def test_base_hook_get_project(self):
        gcp_hook = hook.GoogleCloudBaseHook('gcp_default')
        gcp_hook.extras = {
            'extra__google_cloud_platform__project': 'foo_project',
        }
        self.assertEquals('foo_project', gcp_hook.project_id)


if __name__ == '__main__':
    unittest.main()
