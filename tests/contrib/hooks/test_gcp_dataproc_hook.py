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

import unittest
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

JOB = 'test-job'
PROJECT_ID = 'test-project-id'
REGION = 'global'
TASK_ID = 'test-task-id'

BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
DATAPROC_STRING = 'airflow.contrib.hooks.gcp_dataproc_hook.{}'

def mock_init(self, gcp_conn_id, delegate_to=None):
    pass

class DataProcHookTest(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.dataproc_hook = DataProcHook()

    @mock.patch(DATAPROC_STRING.format('_DataProcJob'))
    def test_submit(self, job_mock):
      with mock.patch(DATAPROC_STRING.format('DataProcHook.get_conn', return_value=None)):
        self.dataproc_hook.submit(PROJECT_ID, JOB)
        job_mock.assert_called_once_with(mock.ANY, PROJECT_ID, JOB, REGION)
